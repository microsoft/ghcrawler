// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const async = require('async');
const Q = require('q');
const URL = require('url');

class GitHubFetcher {

  constructor(requestor, store, tokenFactory, options) {
    this.requestor = requestor;
    this.store = store;
    this.tokenFactory = tokenFactory;
    this.options = options;
    this.logger = options.logger;
    this.options._config.on('changed', this._reconfigure.bind(this));
    this.fetchQueue = async.queue(this._callGitHubTask.bind(this), options.concurrency || 5);
  }

  _reconfigure(current, changes) {
    if (changes.some(patch => patch.path === '/concurrency')) {
      this.fetchQueue.concurrency = this.options.concurrency;
    }
    return Q();
  }

  fetch(request) {
    const initial = request.policy.initialFetch(request);
    if (initial === 'storage') {
      return this._fetchFromStorage(request);
    }
    return this._fetchFromGitHub(request, initial === 'etag');
  }

  _fetchFromGitHub(request, checkEtag) {
    const self = this;
    const etagPromise = checkEtag ? this.store.etag(request.type, request.url) : Q(null);
    return etagPromise.then(etag => {
      let options = self._addTokenOption(request, { headers: {} });
      if (typeof options === 'number') {
        // if we get back a number, all tokens have been benched so we have to requeue and wait.
        return self._requeueBenched(request, options);
      }
      options = self._addEtagOption(options, etag);
      options = self._addTypeOptions(request, options);
      return this._getFromGitHub(request, options).then(response => {
        const status = response.statusCode;
        if (status !== 200 && status !== 304) {
          if (status === 409 || status === 204) {
            return request.markSkip('Empty', `Code ${status} for ${request.url}`);
          }
          // if GitHub is explicitly throttling us (403 and nothing remaining), we missed out on this request, requeue
          // and wait a bit before processing more requests
          const remaining = parseInt(response.headers['x-ratelimit-remaining'], 10) || 0;
          if (status === 403 && remaining === 0) {
            return self._requeueThrottled(request);
          }
          // If response indicates something that may be related to Auth, note the token used and bail if
          // we have already retried. Retries will be done with full permission tokens so if there is still
          // a problem then we are done here.
          if ([401, 403, 404].includes(response.statusCode)) {
            const token = options.headers.authorization.slice(6);
            request.addMeta({ token: `${token.slice(0, 2)}..${token.slice(-2)}` });
            if (request.attemptCount && request.attemptCount > 1) {
              return request.markDead('Bailed', `After ${request.attemptCount} tries.`);
            }
            return request.markRequeue('Missing', 'Requeuing...');
          }
          throw new Error(`Code ${status} for ${request.url}`);
        }

        request.response = response;
        request.contentOrigin = 'origin';
        self._checkGitHubRateLimit(request, response);
        if (status === 304) {
          // We already have the content for this element.  Get the content from the store and process.
          // TODO we may strictly speaking not need to get the content here but it is complicated to tell ahead of time.  Future optimization
          return this.store.get(request.type, request.url).then(document => {
            return this._prepareCachedRequest(request, document, 'cacheOfOrigin');
          });
        }
        request.document = response.body;
        return request;
      });
    });
  }

  _getFromGitHub(request, options) {
    const start = Date.now();
    const deferred = Q.defer();
    const url = this._addTokenToUrl(request, options);
    this.fetchQueue.push({ url: url, options: options }, (error, response) => {
      if (error) {
        return deferred.reject(error);
      }
      request.addMeta({ status: response.statusCode, fetch: Date.now() - start });
      deferred.resolve(response);
    });
    return deferred.promise;
  }

  _addTokenToUrl(request, options) {
    let token = options.headers.authorization;
    if (!token) {
      return request.url;
    }
    const urlSpec = URL.parse(request.url, true);
    urlSpec.query.access_token = token.slice(6);
    delete urlSpec.search;
    delete options.headers.authorization;
    return URL.format(urlSpec);
  }

  _callGitHubTask(spec, callback) {
    try {
      this._incrementMetric('fetch');
      this.requestor.get(spec.url, spec.options).then(
        response =>
          callback(null, response),
        error =>
          callback(error));
    } catch (e) {
      callback(e);
    }
  }

  _requeueBenched(request, benchTime) {
    request.delayUntil(benchTime);
    const benchDelay = benchTime - Date.now();
    request.addMeta({ benchDelay: benchDelay });
    return request.markRequeue('Benched', `Waiting ${benchDelay} for a token`);
  }

  _requeueThrottled(request) {
    const delay = this.options.forbiddenDelay || 120000;
    request.exhaustToken(Date.now() + delay);
    request.delay(delay);
    request.addMeta({ forbiddenDelay: delay });
    return request.markRequeue('Throttled', 'GitHub secondary throttling kicked in');
  }

  _fetchFromStorage(request) {
    const start = Date.now();
    return this.store.get(request.type, request.url).then(
      document => {
        request.addMeta({ read: Date.now() - start });
        request.response = { headers: {} };
        return this._prepareCachedRequest(request, document, 'storage');
      },
      error => {
        // The doc could not be loaded from storage. Either storage has failed somehow or this
        // is a new processing path. Rethrow the error, or use the origin store, respectively.
        const missing = request.policy.shouldFetchMissing(request);
        if (!missing) {
          return request.markSkip('Unreachable for reprocessing');
        }
        return this._fetchFromGitHub(request, false);
      });
  }

  _checkGitHubRateLimit(request, response) {
    const retryAfter = parseInt(response.headers['retry-after'], 10) || 0;
    if (retryAfter > 0) {
      this.logger.info(`Retry-After delay of ${retryAfter} for ${request.toString()}`, request.meta);
      request.addMeta({ retryAfterDelay: retryAfter });
      request.delay(retryAfter * 1000);
    }

    // If we hit the low water mark for requests, proactively sleep until the next ratelimit reset
    // This code is not designed to handle the 403 scenarios.  That is handled by the retry logic.
    const remaining = parseInt(response.headers['x-ratelimit-remaining'], 10) || 0;
    request.addMeta({ remaining: remaining });
    const tokenLowerBound = this.options.tokenLowerBound || 50;
    if (remaining < tokenLowerBound) {
      const reset = parseInt(response.headers['x-ratelimit-reset'], 10) || 0;
      const delay = Math.max(0, reset - Date.now());
      if (delay > 0) {
        request.addMeta({ backoffDelay: delay });
        request.delayUntil(reset);
        request.exhaustToken(reset);
      }
    }
  }

  // tack on any content we want to carry over from the current document for future processing
  _prepareCachedRequest(request, document, origin) {
    const metadata = {
      fetchedAt: document._metadata.fetchedAt,
      version: document._metadata.version
    };
    // Augment the existing repsonse headers with the ones we got last time
    if (document._metadata.headers && Object.getOwnPropertyNames(document._metadata.headers).length > 0) {
      request.response.headers.etag = document._metadata.headers.etag;
      request.response.headers.link = document._metadata.headers.link;
    }
    request.response._metadataTemplate = metadata;
    // be sure to return a "clean" document just like we got from origin the first time (i.e., without metadata)
    // but be careful not to destructively modify the content of the cached document
    request.document = document.elements ? document.elements : Object.assign({}, document);
    delete request.document._metadata;
    request.contentOrigin = origin;
    return request;
  }


  _addEtagOption(options, etag) {
    if (etag) {
      options.headers['If-None-Match'] = etag;
    }
    return options;
  }

  _addTokenOption(request, options) {
    const traits = this._getTypeDetails(request.type).tokenTraits || [];
    const additionalTraits = [];
    if (request.context.repoType) {
      additionalTraits.push(request.context.repoType);
    }
    if (request.attemptCount) {
      // if this is a retry, elevate the token to avoid any permissions issues
      additionalTraits.push('private', 'admin');
    }
    const token = this.tokenFactory.getToken(traits.concat(additionalTraits));
    if (!token) {
      throw new Error(`No API tokens available for ${request.toString()}`);
    }
    if (typeof token === 'number') {
      return token;
    }
    options.headers.authorization = `token ${token}`;
    request.exhaustToken = until => this.tokenFactory.exhaust(token, until);
    return options;
  }

  _addTypeOptions(request, options) {
    const typeDetails = this._getTypeDetails(request.type);
    const headers = typeDetails.headers;
    if (headers) {
      Object.assign(options.headers, headers);
    }
    return options;
  }

  _incrementMetric(operation) {
    const metrics = this.logger.metrics;
    if (metrics && metrics[operation]) {
      metrics[operation].incr();
    }
  }

  _getTypeDetails(type) {
    const result = {
      orgs: { tokenTraits: ['admin'] },
      org: { tokenTraits: ['admin'] },
      repos: { tokenTraits: ['admin'] },
      repo: { tokenTraits: ['admin'] },
      teams: { tokenTraits: ['admin'] },
      team: { tokenTraits: ['admin'] },
      members: { tokenTraits: ['admin'] },
      update_events: { tokenTraits: ['admin'] },
      events: { tokenTraits: ['admin'] },
      collaborators: { tokenTraits: ['push'] },
      outside_collaborators: { tokenTraits: ['push'], Accept: 'application/vnd.github.korra-preview' },
      reactions: { headers: { Accept: 'application/vnd.github.squirrel-girl-preview' } },
      clones: { tokenTraits: ['admin'], headers: { Accept: 'application/vnd.github.spiderman-preview' } },
      referrers: { tokenTraits: ['admin'], headers: { Accept: 'application/vnd.github.spiderman-preview' } },
      views: { tokenTraits: ['admin'], headers: { Accept: 'application/vnd.github.spiderman-preview' } },
      paths: { tokenTraits: ['admin'], headers: { Accept: 'application/vnd.github.spiderman-preview' } }
    }[type];
    return result ? result : {};
  }
}

module.exports = GitHubFetcher;
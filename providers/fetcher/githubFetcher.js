// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Q = require('q');
const URL = require('url');

class GitHubFetcher {

  constructor(requestor, store, tokenFactory, limiter, options) {
    this.requestor = requestor;
    this.store = store;
    this.tokenFactory = tokenFactory;
    this.limiter = limiter;
    this.options = options;
    this.logger = options.logger;
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
      return self._getToken(request).then(token => {
        if (!token) {
          // there were no tokens at all for this request so mark as dead and skip
          return request.markDead('No token', 'No token with matching traits');
        }
        // if we get back a number, all tokens that could address this request have been benched so we have to requeue and wait.
        if (typeof token === 'number') {
          return self._handleDeferred(request, token);
        }
        const options = {
          headers: { authorization: `token ${token}` }
        };
        self._addEtagOption(etag, options);
        self._addTypeOptions(request, options);
        return this._getFromGitHub(request, options).then(response => {
          request.response = response;
          const status = response.statusCode;
          if (status !== 200 && status !== 304) {
            if (status === 409 || status === 204) {
              return request.markSkip('Empty', `Code ${status} for ${request.url}`);
            }
            request.addMeta({ token: token.slice(0, 4) });

            // if GitHub secondary throttling (403 and a retry-after header), requeue and exhaust for the given delay
            if (status === 403 && response.headers['retry-after']) {
              return self._handleSecondaryThrottled(request);
            }

            // if GitHub primary throttling, requeue and exhaust the token until the given reset time
            const remaining = parseInt(response.headers['x-ratelimit-remaining'], 10) || 0;
            if (status === 403 && remaining === 0) {
              return self._handlePrimaryThrottled(request);
            }

            // If response indicates something that may be related to Auth, note the token used and bail if
            // we have already retried. Retries will be done with full permission tokens so if there is still
            // a problem then we are done here.
            if ([401, 403, 404].includes(response.statusCode)) {
              if (request.attemptCount && request.attemptCount > 1) {
                return request.markDead('Bailed', `After ${request.attemptCount} tries. Status ${response.statusCode}`);
              }
              return request.markRequeue('Missing', 'Requeuing...');
            }
            throw new Error(`Code ${status} for ${request.url}`);
          }

          request.contentOrigin = 'origin';
          self._checkGitHubRateLimit(request, response);
          if (status === 304) {
            // We already have the content for this element.  Get the content from the store and process.
            // TODO we may strictly speaking not need to get the content here but it is complicated to tell ahead of time.  Future optimization
            return this.store.get(request.type, request.context.cacheKey || request.url).then(document => {
              return this._prepareCachedRequest(request, document, 'cacheOfOrigin');
            });
          }
          request.document = response.body;
          return request;
        });
      });
    });
  }

  _getFromGitHub(request, options) {
    const [token, url] = this._addTokenToUrl(request, options);
    this._incrementMetric('fetch');
    options.time = true;
    return this.requestor.get(url, options).then(response => {
      const time = response.elapsedTime;
      const key = token.slice(0, 4);
      request.addMeta({ status: response.statusCode, token: key, fetch: time });
      // tally up and consume (or restore) final compute cost for this request.
      return this.limiter.consume(key, time, this.options.defaultComputeCost || 50, request.exhaustToken).then(result => {
        // only log the first time the restore time is changed.
        if (result.updated) {
          this.logger.info('Exceeded ', `Compute limit for token ${key} by ${result.overage}. Benching until ${result.reset - Date.now()}ms from now`, request.meta);
        }
        return response;
      });
    });
  }

  _addTokenToUrl(request, options) {
    let header = options.headers.authorization;
    if (!header) {
      return ['', request.url];
    }
    const urlSpec = URL.parse(request.url, true);
    const token = header.slice(6);
    urlSpec.query.access_token = token;
    delete urlSpec.search;
    delete options.headers.authorization;
    return [token, URL.format(urlSpec)];
  }

  _handleDeferred(request, benchTime) {
    request.delay(this.options.deferDelay || 500);  // add a little delay to this loop just to tame things a bit.
    const delay = benchTime - Date.now();
    request.addMeta({ deferDelay: delay });
    return request.markDefer('Deferred ', `Deferring ${delay}ms for a token`);
  }

  _handleSecondaryThrottled(request) {
    const delay = parseInt(request.response.headers['retry-after'], 10) * 1000;
    request.addMeta({ requestId: `${request.response.headers['x-github-request-id']}` });
    const realDelay = request.exhaustToken(Date.now() + delay);
    request.addMeta({ secondaryDelay: realDelay || 0 });
    return request.markRequeue('Throttled', 'GitHub secondary throttling kicked in');
  }

  _handlePrimaryThrottled(request) {
    // Get the reset time from the response, convert to milliseconds and add a bit of buffer for clock skew.
    const resetTime = parseInt(request.response.headers['x-ratelimit-reset'], 10) * 1000 + 5000;
    request.addMeta({ requestId: `${request.response.headers['x-github-request-id']}` });
    const realDelay = request.exhaustToken(resetTime);
    request.addMeta({ primaryDelay: realDelay || 0 });
    return request.markRequeue('Throttled', 'GitHub primary throttling caught us');
  }

  _fetchFromStorage(request) {
    const start = Date.now();
    return this.store.get(request.type, request.context.cacheKey || request.url).then(
      document => {
        if (!document) {
          return this._fetchMissing(request);
        }
        request.addMeta({ read: Date.now() - start });
        request.response = { headers: {} };
        return this._prepareCachedRequest(request, document, 'storage');
      },
      error => {
        // TODO eating the error here. need to decide what's best as some stores might throw an
        // error to signal missing.
        return this._fetchMissing(request);
      });
  }

  _fetchMissing(request) {
    // The doc could not be loaded from storage. Either storage has failed somehow or this
    // is a new processing path. Decide if we should use the origin store, or skip.
    const missing = request.policy.shouldFetchMissing(request);
    if (missing) {
      return this._fetchFromGitHub(request, false);
    }
    return request.markSkip('Unreachable for reprocessing');
  }

  _checkGitHubRateLimit(request, response) {
    // If we hit the low water mark for requests, proactively sleep until the next ratelimit reset
    // This code is not designed to handle the 403 scenarios.  That is handled by the retry logic.
    const remaining = parseInt(response.headers['x-ratelimit-remaining'], 10) || 0;
    request.addMeta({ remaining: remaining });
    const tokenLowerBound = this.options.tokenLowerBound || 50;
    if (remaining < tokenLowerBound) {
      // we are below the token threshold so back off. Since the response has an absolut reset time,
      // add a few seconds to allow for clock skew.
      const reset = (parseInt(response.headers['x-ratelimit-reset'], 10) || 0) * 1000 + 5000;
      const delay = Math.max(0, reset - Date.now());
      if (delay > 0) {
        request.addMeta({ backoffDelay: delay });
        request.exhaustToken(reset);
      }
    }
  }

  // tack on any content we want to carry over from the current document for future processing
  _prepareCachedRequest(request, document, origin) {
    // Augment the existing repsonse headers with the ones we got last time
    if (document._metadata.headers && Object.getOwnPropertyNames(document._metadata.headers).length > 0) {
      request.response.headers.etag = document._metadata.headers.etag;
      request.response.headers.link = document._metadata.headers.link;
    }
    request.response._metadataTemplate = document._metadata;
    // be sure to return a "clean" document just like we got from origin the first time (i.e., without metadata)
    // but be careful not to destructively modify the content of the cached document
    request.document = document.elements ? document.elements : Object.assign({}, document);
    delete request.document._metadata;
    request.contentOrigin = origin;
    return request;
  }

  _addEtagOption(etag, options) {
    if (etag) {
      options.headers['If-None-Match'] = etag;
    }
    return options;
  }

  _getToken(request) {
    const traits = this._getTypeDetails(request.type).tokenTraits || [];
    let additionalTraits = [];
    if (request.context.repoType) {
      additionalTraits.push(request.context.repoType);
    }
    if (request.attemptCount) {
      // if this is a retry, elevate the token to avoid any permissions issues
      additionalTraits.push('admin');
    }
    additionalTraits = additionalTraits.length === 0 ? [] : [additionalTraits];
    return this.tokenFactory.getToken(additionalTraits.concat(traits)).then(token => {
      if (!token || typeof token === 'number') {
        return token;
      }
      const exhaust = (until => {
        return this.tokenFactory.exhaust(token, until);
      }).bind(this);
      request.exhaustToken = exhaust;
      // allocate some default compute cost so we don't overrun too badly.  Reckoning will be done when the response is in.
      return this.limiter.allocate(token.slice(0, 4), this.options.defaultComputeCost || 50, exhaust).then(result => {
        // only log the first time the restore time is changed.
        if (result.updated) {
          this.logger.info('Exceeded ', `Pre-allocation limit for token ${token.slice(0, 4)} by ${result.overage}. Benching token until ${result.reset - Date.now()}ms from now`);
        }
        if (result.reset) {
          return result.reset;
        }
        return token;
      });
    });
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
      org: { tokenTraits: [['admin'], ['public']] },
      repos: { tokenTraits: [['admin'], ['public']], headers: { Accept: 'application/vnd.github.mercy-preview+json' } },
      repo: { tokenTraits: [['admin'], ['public']], headers: { Accept: 'application/vnd.github.mercy-preview+json' } },
      teams: { tokenTraits: ['admin'] },
      team: { tokenTraits: ['admin'] },
      members: { tokenTraits: ['admin'] },
      events: { tokenTraits: [['admin'], ['public']] },
      collaborators: { tokenTraits: ['admin'], headers: { Accept: 'application/vnd.github.korra-preview' } },
      reviews: { headers: { Accept: 'application/vnd.github.black-cat-preview+json' } },
      review: { headers: { Accept: 'application/vnd.github.black-cat-preview+json' } },
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
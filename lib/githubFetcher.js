const async = require('async');
const jsonpatch = require('fast-json-patch');
const Q = require('q');

class GitHubFetcher {

  constructor(requestor, store, tokenFactory, options) {
    this.requestor = requestor;
    this.store = store;
    this.tokenFactory = tokenFactory;
    this.options = options;
    this.logger = options.logger;
    this.options.reconfigure = this.reconfigure.bind(this);
    this.getQueue = async.queue(this._callGitHubTask.bind(this), options.concurrency || 5);
  }

  reconfigure(patches) {
    // remember options that need processing, apply and then do any necessary processing
    const oldConcurrency = this.options.concurrency;
    jsonpatch.apply(this.options, patches);
    if (oldConcurrency !== this.options.concurrency) {
      this.getQueue.concurrency = this.options.concurrency;
    }
    return Q();
  }

  fetch(request) {
    const initial = request.policy.initialFetch(request);
    if (initial === 'storage') {
      return this._fetchFromStorage(request);
    }
    const checkEtag = request.policy.fetch === 'originStorage';
    return this._fetchFromGitHub(request, checkEtag);
  }

  _fetchFromGitHub(request, checkEtag) {
    const self = this;
    const etagPromise = checkEtag ? this.store.etag(request.type, request.url) : Q(null);
    return etagPromise.then(etag => {
      return this._getFromGitHub(request, etag).then(githubResponse => {
        const status = githubResponse.statusCode;
        if (status !== 200 && status !== 304) {
          if (status === 409 || status === 204) {
            return request.markSkip('Empty resource', `Code ${status} for ${request.url}`);
          }
          // if GitHub is explicitly throttling us (403 and nothing remaining), we missed out on this request, requeue
          // and wait a bit before processing more requests
          const remaining = parseInt(githubResponse.headers['x-ratelimit-remaining'], 10) || 0;
          if (status === 403 && remaining === 0) {
            const delay = self.options.forbiddenDelay || 120000;
            request.exhaustToken(Date.now() + delay);
            request.delay(delay);
            request.addMeta({ forbiddenDelay: delay });
            return request.markRequeue(`GitHub throttled ${request.url}`);
          }
          throw new Error(`Code ${status} for ${request.url}`);
        }

        request.response = githubResponse;
        request.contentOrigin = 'origin';
        self._checkGitHubRateLimit(request, githubResponse);
        if (status === 304) {
          // We already have the content for this element.  If we are forcing, get the content from the
          // store and process.  Otherwise, skip.
          if (request.shouldFetchExisting()) {
            return this.store.get(request.type, request.url).then(document => {
              return this._prepareCachedRequest(request, document, 'cacheOfOrigin');
            });
          }
          return request.markSkip('Unmodified');
        }
        request.document = githubResponse.body;
        return request;
      });
    });
  }

  _getFromGitHub(request, etag) {
    const options = this._getOptions(request);
    if (etag) {
      options.headers['If-None-Match'] = etag;
    }
    const start = Date.now();
    const deferred = Q.defer();
    this.getQueue.push({ url: request.url, options: options }, (error, response) => {
      if (error) {
        return deferred.reject(error);
      }
      // If response indicates something that may be related to  Auth, note the token used
      if ([401, 403, 404].includes(response.statusCode)) {
        const token = options.headers.authorization.slice(6);
        request.addMeta({ token: `${token.slice(0, 2)}..${token.slice(-2)}` });
      }
      request.addMeta({ status: response.statusCode, fetch: Date.now() - start });
      deferred.resolve(response);
    });
    return deferred.promise;
  }

  _callGitHubTask(spec, callback) {
    this.requestor.get(spec.url, spec.options).then(
      response => callback(null, response),
      error => callback(error));
  }

  _fetchFromStorage(request) {
    return this.store.get(request.type, request.url).then(
      document => {
        request.response = { headers: {} };
        return this._prepareCachedRequest(request, document, 'storage');
      },
      error => {
        // The doc could not be loaded from storage. Either storage has failed somehow or this
        // is a new processing path. Rethrow the error, or use the origin store, respectively.
        const missing = request.policy.missingFetch(request);
        if (!missing) {
          return request.markSkip('Unreachable for reprocessing');
        }
        return this._fetchFromGitHub(request, false);
      });
  }

  _checkGitHubRateLimit(request, response) {
    const retryAfter = parseInt(response.headers['retry-after'], 10) || 0;
    if (retryAfter > 0) {
      this.logger.info(`Retry-After delay of ${retryAfter} for ${request.toString()}`);
      request.addMeta({ retryAfterDelay: retryAfter });
      request.delay(retryAfter * 1000);
    }

    // If we hit the low water mark for requests, proactively sleep until the next ratelimit reset
    // This code is not designed to handle the 403 scenarios.  That is handled by the retry logic.
    const remaining = parseInt(response.headers['x-ratelimit-remaining'], 10) || 0;
    request.addMeta({ remaining: remaining });
    const tokenLowerBound = this.options ? (this.options.tokenLowerBound || 50) : 50;
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
    delete document._metadata;
    request.document = document.elements ? document.elements : document;
    request.contentOrigin = origin;
    return request;
  }

  _getOptions(request) {
    const result = { headers: {} };
    const token = this._getToken(request);
    if (token) {
      result.headers.authorization = `token ${token}`;
      request.exhaustToken = until => this.tokenFactory.exhaust(token, until);
    } else {
      throw new Error(`No API tokens available for ${request.toString()}`);
    }
    const headers = this._getHeaders(request);
    if (headers) {
      Object.assign(result.headers, headers);
    }
    return result;
  }

  _getToken(request) {
    const traits = this._getTypeDetails(request.type).tokenTraits || [];
    const repoType = request.context.repoType;
    return this.tokenFactory.getToken(traits.concat(repoType ? [repoType] : []));
  }

  _getHeaders(request) {
    const typeDetails = this._getTypeDetails(request.type);
    return typeDetails.headers;
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
      collaborators: { tokenTraits: ['push'] },
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
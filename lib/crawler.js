const extend = require('extend');
const moment = require('moment');
const Processor = require('./processor');
const Q = require('q');
const Request = require('./request');
const URL = require('url');

class Crawler {
  constructor(queues, store, locker, requestor, config, logger) {
    this.queues = queues;
    this.store = store;
    this.locker = locker;
    this.requestor = requestor;
    this.config = config;
    this.logger = logger;
    this.processor = new Processor();
  }

  start(name) {
    let requestBox = [];

    return Q()
      .then(this.log(this._getRequest.bind(this, requestBox, name)))
      .then(this.log(this._filter.bind(this)))
      .then(this.log(this._fetch.bind(this)))
      .then(this.log(this._convertToDocument.bind(this)))
      .then(this.log(this._processDocument.bind(this)))
      .then(this.log(this._storeDocument.bind(this)))
      .catch(this.log(this._errorHandler.bind(this, requestBox)))
      .then(this.log(this._completeRequest.bind(this), this._completeRequest.bind(this)))
      .catch(this.log(this._errorHandler.bind(this, requestBox)))
      .then(this.log(this._logOutcome.bind(this)))
      .then(this.log(this._startNext.bind(this, name), this._startNext.bind(this, name)));
  }

  _errorHandler(requestBox, error) {
    if (requestBox[0]) {
      return requestBox[0].markRequeue('Error', error);
    }
    const request = new Request('_errorTrap', null);
    request.markDelay();
    request.markSkip('Error', error);
    requestBox[0] = request;
    return request;
  }

  _getRequest(requestBox, name) {
    const self = this;
    return this.log(this.queues.pop())
      .then(request => {
        if (!request) {
          request = new Request('_blank', null);
          request.markDelay();
          request.markSkip('Exhausted queue', `Waiting 1 second`);
        }
        request.start = Date.now();
        request.crawler = self;
        request.crawlerName = name;
        requestBox[0] = request;
        request.promises = [];
        return request;
      })
      .then(self.log(self._acquireLock.bind(self)));
  }

  _acquireLock(request) {
    if (!request.url || !this.locker) {
      return Q(request);
    }
    const self = this;
    return Q.try(() => {
      return this.log(self.locker.lock(request.url, 1 * 60 * 1000), 'lock');
    }).then(
      lock => {
        request.lock = lock;
        return request;
      },
      error => {
        // If we could not acquire a lock, abandon the request so it will be returned to the queue.
        //  If that fails, throw the original error
        return Q.try(() => {
          this.log(this.queues.abandon(request), 'abandon');
        }).finally(() => {
          // don't throw if it is the normal Exceeded scenario. It is not an "error", just someone else is processing.
          if (error.message.startsWith('Exceeded')) {
            request.markRequeue('Requeued', `Could not lock ${request.url}`);
            return request;
          }
          throw error;
        });
      });
  }

  _releaseLock(request) {
    if (!request.lock || !this.locker) {
      return Q(request);
    }
    const self = this;
    return Q.try(() => {
      return this.locker.unlock(request.lock);
    }).then(
      () => {
        request.lock = null;
        return request;
      },
      error => {
        request.lock = null;
        self.logger.error(error);
        return request;
      });
  }

  _completeRequest(request) {
    // requeue the request if needed then wait for any accumulated promises and release the lock and clean up the queue
    this._requeue(request);
    const self = this;
    return Q.all(request.promises)
      .finally(() => self._releaseLock(request))
      .finally(() => self._deleteFromQueue(request))
      .then(() => request);
  }

  _requeue(request) {
    if (!request.shouldRequeue() || !request.url) {
      return;
    }
    request.attemptCount = request.attemptCount || 0;
    if (++request.attemptCount > 5) {
      this.logger.warn(`Exceeded attempt count for ${request.type} ${request.url}`);
      request.track(this._queueDead(request, request));
    } else {
      request.addMeta({ attempt: request.attemptCount });
      this.logger.verbose(`Requeuing attempt ${request.attemptCount} of request ${request.type} for ${request.url}`);
      request.track(this._requeueOrigin(request));
    }
  }

  _startNext(name, request) {
    const now = Date.now();
    let delay = 0;
    if (request) {
      const requestGate = now + (request.shouldDelay() ? 1000 : 0);
      const delayGate = request.nextRequestTime || now;
      const nextRequestTime = Math.max(requestGate, delayGate, now);
      delay = Math.max(0, nextRequestTime - now);
    }
    if (delay) {
      this.logger.verbose(`Crawler: ${name} waiting for ${delay}ms`);
    }
    setTimeout(() => {
      try {
        this.start(name);
      } catch (error) {
        // If for some reason we throw all the way out of start, log and restart the loop
        this.logger.error(new Error('PANIC! Crawl loop exited unexpectedly'));
        this.logger.error(error);
        this._startNext(name, null);
      }
    }, delay);
  }

  _filter(request) {
    if (!this._shouldInclude(request.type, request.url)) {
      request.markSkip('Filtered');
    }
    return request;
  }

  _fetch(request) {
    if (request.shouldSkip()) {
      return request;
    }
    // rewrite the request type for collections remember the collection subType
    // Also setup 'page' as the document type to look up for etags etc.
    let fetchType = request.type;
    let subType = request.getCollectionType();
    if (subType) {
      request.type = 'collection';
      request.subType = subType;
      fetchType = 'page';
    }
    const self = this;
    return this.store.etag(fetchType, request.url).then(etag => {
      const options = etag ? { headers: { 'If-None-Match': etag } } : {};
      const start = Date.now();
      return self.requestor.get(request.url, options).then(githubResponse => {
        const status = githubResponse.statusCode;
        request.addMeta({ status: status, fetch: Date.now() - start });
        if (status !== 200 && status !== 304) {
          if (status === 409) {
            return request.markSkip('Empty repo', `Code: ${status} for: ${request.url}`);
          }
          // if GitHub is explicitly throttling us, we missed out on this request, requeue
          // and wait a couple minutes before processing more requests
          if (status === 403) {
            const delay = 2 * 60 * 1000;
            request.delayFor(delay);
            request.addMeta({ forbiddenDelay: delay });
            return request.markRequeue('GitHub throttled: ${request.url}');
          }
          throw new Error(`Code: ${status} for: ${request.url}`);
        }

        self._checkGitHubRateLimit(request, githubResponse);
        if (status === 304) {
          // We have the content for this element.  If we are forcing, get the content from the
          // store and process.  Otherwise, skip.
          if (!request.force) {
            return request.markSkip('Unmodified');
          }
          return self.store.get(fetchType, request.url).then(document => {
            // if the doc had stored headers (e.g., page responses) then reconstitute them for processing
            if (document._metadata && document._metadata.headers) {
              Object.assign(githubResponse.headers, document._metadata.headers);
            }
            request.document = document;
            request.response = githubResponse;
            // Our store is up to date so don't store
            request.store = false;
            return request;
          });
        }
        request.document = githubResponse.body;
        request.response = githubResponse;
        return request;
      });
    });
  }

  _convertToDocument(request) {
    if (request.shouldSkip()) {
      return Q(request);
    }

    // If the doc is an array,
    // * wrap it in an object to make storage more consistent (Mongo can't store arrays directly)
    // * save the link header as GitHub will not return those in a subsequent 304
    let headers = {};
    if (Array.isArray(request.document)) {
      request.document = { elements: request.document };
      headers = { link: request.response.headers.link };
    }
    request.document._metadata = {
      type: request.type,
      url: request.url,
      etag: request.response.headers.etag,
      fetchedAt: moment.utc().toISOString(),
      links: {},
      headers: headers
    };
    return Q(request);
  }

  _processDocument(request) {
    if (request.shouldSkip()) {
      return Q(request);
    }
    request.document = this.processor.process(request);
    return Q(request);
  }

  _storeDocument(request) {
    // See if we should skip storing the document.  Test request.store explicitly for false as it may just not be set.
    if (request.shouldSkip() || !this.store || !request.document || request.store === false) {
      return Q(request);
    }

    return this.store.upsert(request.document).then(upsert => {
      request.upsert = upsert;
      return request;
    });
  }

  _deleteFromQueue(request) {
    return this.queues.done(request).then(() => { return request; });
  }

  _logOutcome(request) {
    const outcome = request.outcome ? request.outcome : 'Processed';
    if (outcome === 'Error') {
      const error = (request.message instanceof Error) ? request.message : new Error(request.message);
      error.request = request;
      this.logger.error(error);
    } else {
      request.addMeta({ time: Date.now() - request.start });
      this.logger.info(`${outcome} ${request.type} [${request.url}] ${request.message || ''}`, request.meta);
    }
    return request;
  }

  // ===============  Helpers  ============

  _requeueOrigin(request) {
    const queuable = this._createQueuable(request);
    return this.queues.repush(request, queuable);
  }

  _queueDead(request) {
    const queuable = this._createQueuable(request);
    return this.queues.pushDead(queuable);
  }

  queue(request, priority = false) {
    if (!this._shouldInclude(request.type, request.url)) {
      this.logger.verbose(`Filtered ${request.type} [${request.url}]`);
      return [];
    }
    const queuable = this._createQueuable(request);
    return priority ? this.queues.pushPriority(queuable) : this.queues.push(queuable);
  }

  _createQueuable(request) {
    // Create a new request data structure that has just the things we should queue
    const queuable = new Request(request.type, request.url);
    queuable.attemptCount = request.attemptCount;
    queuable.context = request.context;
    queuable.force = request.force;
    queuable.subType = request.subType;
    return queuable;
  }

  _shouldInclude(type, target) {
    if (!this.config.orgFilter || this.config.orgFilter.size === 0) {
      return true;
    }
    if (type === 'repo' || type === 'repos' || type === 'org') {
      const parsed = URL.parse(target);
      const org = parsed.path.split('/')[2];
      return this.config.orgFilter.has(org.toLowerCase());
    }
    return true;
  }

  _checkGitHubRateLimit(request, response) {
    const retryAfter = parseInt(response.headers['Retry-After']) || 0;
    if (retryAfter > 0) {
      request.addMeta({ retryAfterDelay: retryAfter });
      request.delayFor(retryAfter * 1000);
    }

    // If we hit the low water mark for requests, proactively sleep until the next ratelimit reset
    // This code is not designed to handle the 403 scenarios.  That is handled by the retry logic.
    const remaining = parseInt(response.headers['x-ratelimit-remaining']) || 0;
    request.addMeta({ remaining: remaining });
    const tokenLowerBound = this.config ? (this.config.tokenLowerBound || 50) : 50;
    if (remaining < tokenLowerBound) {
      const reset = parseInt(response.headers['x-ratelimit-reset']) || 0;
      const delay = Math.max(0, reset - Date.now());
      if (delay > 0) {
        request.addMeta({ backoffDelay: delay });
        request.delayUntil(reset);
      }
    }
  }

  // don't mess with the funky method signature formatting.  You need spaces around the
  // istanbul comment for istanbul to pick it up but auto code formatting removes the spaces
  // before the (.  Putting a newline seems to keep everyone happy.
  log /* istanbul ignore next */
    (thing) {
    if (!this.config.promiseTrace) {
      return thing;
    }
    const self = this;
    if (typeof thing === 'function') {
      return function () {
        const args = array_slice(arguments);
        const name = thing.name.replace('bound ', '');
        self.logger.verbose(`Promise Function Enter: ${name}`);
        const result = thing.apply(self, args);
        if (typeof result.then === 'function') {
          result.then(
            result => { self.logger.silly(`Promise Function Success: ${name}`); },
            error => { self.logger.silly(`Promise Function Error: ${name}`, error); });
        } else {
          self.logger.verbose(`Promise Function Exit: ${name}: ${result}`);
        }
        return result;
      };
    } else if (typeof thing.then === 'function') {
      this.logger.silly(`Promise Enter`);
      thing.then(
        result => { this.logger.silly(`Promise Success: ${result}`); },
        error => { this.logger.silly(`Promise Error: ${result}`, error); });
      return thing;
    }
  }
}

module.exports = Crawler;

/* istanbul ignore next */
let call = Function.call;
/* istanbul ignore next */
function uncurryThis(f) {
  return function () {
    return call.apply(f, arguments);
  };
}
// This is equivalent, but slower:
// uncurryThis = Function_bind.bind(Function_bind.call);
// http://jsperf.com/uncurrythis


/* istanbul ignore next */
let array_slice = uncurryThis(Array.prototype.slice);


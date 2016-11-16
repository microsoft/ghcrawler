const extend = require('extend');
const moment = require('moment');
const Processor = require('./processor');
const Q = require('q');
const Request = require('./request');
const URL = require('url');

class Crawler {
  constructor(queue, priorityQueue, deadletterQueue, store, locker, requestor, config, logger) {
    this.normalQueue = queue;
    this.priorityQueue = priorityQueue;
    this.deadletterQueue = deadletterQueue;
    this.store = store;
    this.locker = locker;
    this.requestor = requestor;
    this.config = config;
    this.logger = logger;
    this.processor = new Processor();
  }

  log(thing) {
    const args = array_slice(arguments, 1);
    const self = this;
    if (typeof thing === 'function') {
      return () => {
        this.logger.verbose(`Enter: ${thing.name}`);
        const result = thing.apply(self, args);
        if (typeof result.then === 'function') {
          result.then(
            result => { this.logger.verbose(`Success: ${thing.name}`); },
            error => { this.logger.error(`Error: ${thing.name}`, error); });
        } else {
          this.logger.verbose(`Exit: ${thing.name} : ${result}`);
        }
        return result;
      };
    } else if (typeof thing.then === 'function') {
      this.logger.verbose(`Enter: ${message}`);
      thing.then(
        result => { this.logger.verbose(`Success: ${result} : ${message}`); },
        error => { this.logger.error(`Error: ${result} : ${message}`, error); });
      return thing;
    }
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
    return Q()
      .then(self.log(self._pop.bind(self, self.priorityQueue)))
      .then(self.log(self._pop.bind(self, self.normalQueue)))
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

  _pop(queue, request = null) {
    return Q.try(() => {
      return request ? request : queue.pop();
    }).then(result => {
      if (result && !result.originQueue) {
        result.originQueue = queue;
      }
      return result;
    });
  }

  _acquireLock(request) {
    if (!request.url || !this.locker) {
      return Q(request);
    }
    const self = this;
    return Q.try(() => {
      return this.log(self.locker.lock(request.url, 5 * 60 * 1000), 'lock');
    }).then(
      lock => {
        request.lock = lock;
        return request;
      },
      error => {
        // If we could not acquire a lock, abandon the request so it will be returned to the queue.
        //  If that fails, throw the original error
        return Q.try(() => {
          this.log(request.originQueue.abandon(request), 'abandon');
        }).finally(() => { throw error; });
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
      this.queue(request, request, this.deadletterQueue);
    } else {
      request.addMeta({ attempt: request.attemptCount });
      this.logger.verbose(`Requeuing attempt ${request.attemptCount} of request ${request.type} for ${request.url}`);
      this.queue(request, request, request.originQueue);
    }
  }

  _startNext(name, request) {
    const now = Date.now();
    const requestGate = now + (request.shouldDelay() ? 1000 : 0);
    const delayGate = self.delayUntil || now;
    const nextRequestTime = Math.max(requestGate, delayGate, now);
    const delay = Math.max(0, nextRequestTime - now);
    setTimeout(this.start.bind(this, name), delay);
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
            self._delayFor(delay);
            request.addMeta({ forbiddenDelay: delay});
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

    // If the doc is an array, wrap it in an object to make storage more consistent (Mongo can't store arrays directly)
    if (Array.isArray(request.document)) {
      request.document = { elements: request.document };
    }
    request.document._metadata = {
      type: request.type,
      url: request.url,
      etag: request.response.headers.etag,
      fetchedAt: moment.utc().toISOString(),
      links: {}
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
    return request.originQueue.done(request).then(() => request);
  }

  _logOutcome(request) {
    const outcome = request.outcome ? request.outcome : 'Processed';
    if (outcome === 'Error') {
      const error = (request.message instanceof Error) ? request.message : new Error(request.message);
      error.request = request;
      this.logger.error(error);
    } else {
      request.addMeta({ total: Date.now() - request.start });
      this.logger.info(`${outcome} ${request.type} [${request.url}] ${request.message || ''}`, request.meta);
    }
    return request;
  }

  // ===============  Helpers  ============

  queue(request, newRequest, queue = null) {
    if (!this._shouldInclude(newRequest.type, newRequest.url)) {
      this.logger.verbose(`Filtered ${newRequest.type} [${newRequest.url}]`);
      return;
    }

    // Create a new request data structure that has just the things we should queue
    const queuable = new Request(newRequest.type, newRequest.url);
    queuable.attemptCount = newRequest.attemptCount;
    queuable.context = newRequest.context;
    queuable.force = newRequest.force;
    queuable.subType = newRequest.subType;

    queue = queue || this.normalQueue;
    request.promises.push(queue.push(queuable));
    return request;
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
      this._deleteFor(retryAfter * 1000);
    }

    // If we hit the low water mark for requests, proactively sleep until the next ratelimit reset
    // This code is not designed to handle the 403 scenarios.  That is handled by the retry logic.
    const remaining = parseInt(response.headers['x-ratelimit-remaining']) || 0;
    const tokenLowerBound = this.config ? (this.config.tokenLowerBound || 50) : 50;
    if (remaining < tokenLowerBound) {
      const reset = parseInt(response.headers['x-ratelimit-reset']) || 0;
      const delay = Math.max(0, Date.now - reset);
      if (delay > 0) {
        request.addMeta({ backoffDelay: delay });
        this._delayUntil(reset);
      }
    }
  }

  _delayUntil(time) {
    if (!this.delayUntil || this.delayUntil < time) {
      this.delayUntil = time;
    }
  }

  _delayFor(milliseconds) {
    this._delayUntil(Date.now() + milliseconds);
  }

}

module.exports = Crawler;

let call = Function.call;
function uncurryThis(f) {
  return function () {
    return call.apply(f, arguments);
  };
}
// This is equivalent, but slower:
// uncurryThis = Function_bind.bind(Function_bind.call);
// http://jsperf.com/uncurrythis


let array_slice = uncurryThis(Array.prototype.slice);


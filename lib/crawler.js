const extend = require('extend');
const moment = require('moment');
const Processor = require('./processor');
const Q = require('q');
const Request = require('./request');
const URL = require('url');

class Crawler {
  constructor(queues, store, locker, requestor, options) {
    this.queues = queues;
    this.store = store;
    this.locker = locker;
    this.requestor = requestor;
    this.options = options;
    this.logger = options.logger;
    this.processor = new Processor();
  }

  run(context) {
    let delay = context.delay;
    if (delay === -1) {
      // We are done call the done handler and return without continuing the loop
      return context.done ? context.done() : null;
    }
    if (delay) {
      this.logger.verbose(`Crawler: ${context.name} waiting for ${delay}ms`);
    }
    setTimeout(() => { this._run(context); }, delay);
  }

  _run(context) {
    try {
      // if this loop got cancelled while sleeping, exit
      if (context.delay === -1) {
        return context.done ? context.done() : null;
      }
      return Q.try(() => this.processOne(context))
        .then(this.log(this._computeDelay.bind(this, context)), this._panic.bind(this, context))
        .finally(this.log(this.run.bind(this, context)));
    } catch (error) {
      // If for some reason we throw all the way out of start, log and restart the loop
      this._panic(context, error);
      this.run(context);
    }
  }

  _panic(context, error) {
    this.logger.error(new Error('PANIC, we should not have gotten here'));
    this.logger.error(error);
  }

  _computeDelay(context, request) {
    let delay = context.delay;
    if (delay === -1) {
      return delay;
    }
    delay = delay || 0;
    const now = Date.now();
    const contextGate = now + delay;
    const requestGate = request.nextRequestTime || now;
    const nextRequestTime = Math.max(contextGate, requestGate, now);
    delay = Math.max(0, nextRequestTime - now);
    context.currentDelay = delay;
    return delay;
  }

  /**
   * Process one request cycle.  If an error happens during processing, handle it there and
   * return a spec describing any delays that should be in .
   */
  processOne(context) {
    let requestBox = [];

    return Q()
      .then(this.log(this._getRequest.bind(this, requestBox, context)))
      .then(this.log(this._filter.bind(this)))
      .then(this.log(this._fetch.bind(this)))
      .then(this.log(this._convertToDocument.bind(this)))
      .then(this.log(this._processDocument.bind(this)))
      .then(this.log(this._storeDocument.bind(this)))
      .catch(this.log(this._errorHandler.bind(this, requestBox)))
      .then(this.log(this._completeRequest.bind(this), this._completeRequest.bind(this)))
      .catch(this.log(this._errorHandler.bind(this, requestBox)))
      .then(this.log(this._logOutcome.bind(this)))
      .catch(this.log(this._errorHandler.bind(this, requestBox)));
  }

  _errorHandler(requestBox, error) {
    if (requestBox[0]) {
      if (requestBox[0].type === '_errorTrap') {
        // TODO if there is a subsequent error, just capture the first and carry on for now.  likely should log
        return requestBox[0];
      } else {
        return requestBox[0].markRequeue('Error', error);
      }
    }
    const request = new Request('_errorTrap', null);
    request.delay();
    request.markSkip('Error', error);
    requestBox[0] = request;
    return request;
  }

  _getRequest(requestBox, context) {
    const self = this;
    return this.log(this.queues.pop())
      .then(request => {
        if (!request) {
          request = new Request('_blank', null);
          request.delay(2000);
          request.markSkip('Exhausted queue', `Waiting 2 seconds`);
        }
        request.start = Date.now();
        request.crawler = self;
        request.loopName = context.name;
        requestBox[0] = request;
        request.context = request.context || {};
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
      this.logger.info(`Requeuing attempt ${request.attemptCount} of request ${request.type} for ${request.url}`);
      request.track(this._requeueOrigin(request));
    }
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
    const self = this;
    if (request.isReprocessing()) {
      return this._fetchFromStore(request, request.type, true);
    }
    return this._getEtag(request, request.type).then(etag => {
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
            request.delay(delay);
            request.addMeta({ forbiddenDelay: delay });
            return request.markRequeue(`GitHub throttled: ${request.url}`);
          }
          throw new Error(`Code: ${status} for: ${request.url}`);
        }

        request.response = githubResponse;
        self._checkGitHubRateLimit(request, githubResponse);
        if (status === 304) {
          // We have the content for this element.  If we are forcing, get the content from the
          // store and process.  Otherwise, skip.
          if (request.isForced()) {
            return self._fetchFromStore(request, request.type);
          }
          return request.markSkip('Unmodified');
        }
        request.document = githubResponse.body;
        request.response = githubResponse;
        return request;
      });
    });
  }

  _getEtag(request, fetchType) {
    if (request.isForcedFetch()) {
      return Q(null);
    }
    return this.store.etag(fetchType, request.url);
  }

  _fetchFromStore(request, fetchType, storeFlag = false) {
    return this.store.get(fetchType, request.url).then(document => {
      // if the doc had stored headers (e.g., page responses) then reconstitute them for processing
      if (document._metadata && document._metadata.headers) {
        Object.assign(request.response.headers, document._metadata.headers);
      }
      // Undo any processing that may have been stored with the doc
      request.document = document.elements ? document.elements : document;
      delete document._metadata;
      request.store = storeFlag;
      return request;
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

    const start = Date.now();
    return this.store.upsert(request.document).then(upsert => {
      request.upsert = upsert;
      request.addMeta({ store: Date.now() - start });
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

  queue(request, name = 'normal') {
    if (!this._shouldInclude(request.type, request.url)) {
      this.logger.verbose(`Filtered ${request.type} [${request.url}]`);
      return [];
    }
    const queuable = this._createQueuable(request);
    return this.queues.push(queuable, name);
  }

  _createQueuable(request) {
    // Create a new request data structure that has just the things we should queue
    const queuable = new Request(request.type, request.url, request.context);
    queuable.attemptCount = request.attemptCount;
    queuable.transitivity = request.transitivity || 'normal';
    queuable.fetch = request.fetch || 'normal';
    return queuable;
  }

  _shouldInclude(type, target) {
    if (!this.options.orgList || this.options.orgList.length === 0) {
      return true;
    }
    if (type === 'repo' || type === 'repos' || type === 'org') {
      const parsed = URL.parse(target);
      const org = parsed.path.split('/')[2];
      return this.options.orgList.includes(org.toLowerCase());
    }
    return true;
  }

  _checkGitHubRateLimit(request, response) {
    const retryAfter = parseInt(response.headers['Retry-After']) || 0;
    if (retryAfter > 0) {
      request.addMeta({ retryAfterDelay: retryAfter });
      request.delay(retryAfter * 1000);
    }

    // If we hit the low water mark for requests, proactively sleep until the next ratelimit reset
    // This code is not designed to handle the 403 scenarios.  That is handled by the retry logic.
    const remaining = parseInt(response.headers['x-ratelimit-remaining']) || 0;
    request.addMeta({ remaining: remaining });
    const tokenLowerBound = this.options ? (this.options.tokenLowerBound || 50) : 50;
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
    if (!this.options.promiseTrace) {
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


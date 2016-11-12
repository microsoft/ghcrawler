const extend = require('extend');
const moment = require('moment');
const Processor = require('./processor');
const Q = require('q');
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

  start() {
    let requestBox = [];

    return this._getRequest(requestBox)
      .then(this._filter.bind(this))
      .then(this._fetch.bind(this))
      .then(this._convertToDocument.bind(this))
      .then(this._processDocument.bind(this))
      .then(this._storeDocument.bind(this))
      .catch(this._errorHandler.bind(this, requestBox))
      .then(this._completeRequest.bind(this), this._completeRequest.bind(this))
      .catch(this._errorHandler.bind(this, requestBox))
      .then(this._logOutcome.bind(this))
      .then(this._startNext.bind(this), this._startNext.bind(this));
  }

  _errorHandler(requestBox, error) {
    if (requestBox[0]) {
      return requestBox[0].markRequeue('Error', error);
    }
    const request = new request('wait', null);
    request.markDelay();
    requestBox[0] = request;
    return request;
  }

  _getRequest(requestBox) {
    return this._pop(this.priorityQueue)
      .then(this._pop.bind(this, this.normalQueue))
      .then(request => {
        if (!request) {
          request = new request('wait', null);
          request.markDelay();
          request.markSkip('Exhausted queue', `Waiting 1 second`);
        }
        request.start = Date.now();
        requestBox[0] = request;
        return request;
      })
      .then(this._acquireLock.bind(this));
  }

  _acquireLock(request) {
    if (request.url) {
      return this.locker.lock(request.url, 5 * 60 * 1000).then((lock) => {
        request.lock = lock;
        return request;
      }, error => {
        return request.originQueue.abandon(request).finally(() => {
          throw error;
        });
      });
    }
  }

  _releaseLock(request) {
    if (request.lock) {
      return this.locker.unlock(request.lock)
        .then(() => {
          return request;
        }, error => {
          this.logger.error(error);
          return request;
        });
    }
    return Q(request);
  }

  _completeRequest(request) {
    return this._releaseLock(request).finally(() => {
      if (request.shouldRequeue()) {
        request.attemptCount = request.attemptCount || 1;
        if (++request.attemptCount > 5) {
          this.logger.warn(`Exceeded attempt count for ${request.type} ${request.url}`);
          this.queue(request, request, this.deadletterQueue);
        } else {
          request.addMeta({ attempt: request.attemptCount });
          this.logger.verbose(`Requeuing attempt ${request.attemptCount} of request ${request.type} for ${request.url}`);
          this.queue(request, request, request.originQueue);
        }
      }
      return this._deleteFromQueue(request);
    })
  }

  _pop(queue, request = null) {
    const self = this;
    return (request ? Q(request) : queue.pop()).then(result => {
      if (result) {
        result.crawler = self;
        result.originQueue = queue;
      }
      return result;
    });
  }

  _startNext(request) {
    const delay = request.shouldDelay() ? 1000 : 0;
    setTimeout(this.start.bind(this), delay);
  }

  _filter(request) {
    if (this._configFilter(request.type, request.url)) {
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
          throw new Error(`Code: ${status} for: ${request.url}`);
        }

        if (status === 304 && githubResponse.headers.etag === etag) {
          // We have the content for this element.  If it is immutable, skip.
          // Otherwise get it from the store and process.
          if (!request.force) {
            return request.markSkip('Unmodified');
          }
          return self.store.get(fetchType, request.url).then(document => {
            request.document = document;
            request.response = githubResponse;
            // Our store is up to date so don't
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
      return Q.resolve(request);
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
    request.promises = [];
    return Q.resolve(request);
  }

  _processDocument(request) {
    if (request.shouldSkip()) {
      return Q.resolve(request);
    }
    let handler = this.processor[request.type];
    handler = handler || this[request.type];
    if (!handler) {
      return request.markSkip('Warning', `No handler found for request type: ${request.type}`);
    }

    request.document = handler.call(this.processor, request);
    return Q.resolve(request);
  }

  _storeDocument(request) {
    // See if we should skip storing the document.  Test request.store explicitly for false as it may just not be set.
    if (request.shouldSkip() || !this.store || !request.document || request.store === false) {
      return Q.resolve(request);
    }

    return this.store.upsert(request.document).then((upsert) => {
      request.upsert = upsert;
      return request;
    });
  }

  _deleteFromQueue(request) {
    if (!request.message) {
      return Q.resolve(request);
    }
    return this.normalQueue.done(request).then(() => { return request; });
  }

  _logOutcome(request) {
    const outcome = request.outcome ? request.outcome : 'Processed';
    if (outcome === 'Error') {
      this.logger.error(request.message);
    } else {
      request.addMeta({ total: Date.now() - request.start });
      this.logger.info(`${outcome} ${request.type} [${request.url}] ${request.message || ''}`, request.meta);
    }
    return request;
  }

  // ===============  Helpers  ============

  // TODO make a queue all and add promises (then) to the code below
  queue(request, newRequest, queue = null) {
    if (this._configFilter(newRequest.type, newRequest.url)) {
      this.logger.verbose(`Filtered ${newRequest.type} [${newRequest.url}]`);
      return;
    }
    queue = queue || this.normalQueue;
    request.promises.push(queue.push(newRequest));
    return request;
  }

  _configFilter(type, target) {
    if (!this.config.orgFilter) {
      return false;
    }
    if (type === 'repo' || type === 'repos' || type === 'org') {
      const parsed = URL.parse(target);
      const org = parsed.path.split('/')[2];
      return !this.config.orgFilter.has(org.toLowerCase());
    }
    return false;
  }
}

module.exports = Crawler;
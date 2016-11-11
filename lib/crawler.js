const extend = require('extend');
const moment = require('moment');
const parse = require('parse-link-header');
const Processor = require('./processor');
const Q = require('q');
const URL = require('url');

class Crawler {
  constructor(queue, priorityQueue, store, requestor, config, logger) {
    this.queue = queue;
    this.priorityQueue = priorityQueue;
    this.store = store;
    this.requestor = requestor;
    this.config = config;
    this.logger = logger;
    this.processor = new Processor();
  }

  start() {
    return this._pop(this.priorityQueue)
      .then(this._pop.bind(this, this.queue))
      .then(this._trackStart.bind(this))
      .then(this._filter.bind(this))
      .then(this._fetch.bind(this))
      .then(this._convertToDocument.bind(this))
      .then(this._processDocument.bind(this))
      .then(this._storeDocument.bind(this))
      .then(this._deleteFromQueue.bind(this))
      .then(this._logOutcome.bind(this))
      .then(this._startNext.bind(this))
      .catch(error => {
        this.logger.log('error', `${error.message}`);
      });
  }

  _pop(queue, request = null) {
    const self = this;
    return (request ? Q(request) : queue.pop()).then(result => {
      if (result) {
        result.crawler = self;
      }
      return result;
    });
  }

  _trackStart(request) {
    request.start = Date.now();
    return Q(request);
  }

  _startNext() {
    setTimeout(this.start.bind(this), 0);
  }

  _filter(request) {
    if (this._configFilter(request.type, request.url)) {
      request.markSkip('Filtered');
    }
    return Q.resolve(request);
  }

  _fetch(request) {
    if (request.skip) {
      return Q.resolve(request);
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
          request.markSkip('Error', new Error(`Code: ${status} for: ${request.url}`));
          return request;
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
            // Our store is up to date so don't '
            request.store = false;
            return request;
          });
        }
        request.document = githubResponse.body;
        request.response = githubResponse;
        return request;
      });
    }).catch(error => {
      // TODO can this request be requeued?
      return request.markSkip('Error', error);
    });
  }

  _convertToDocument(request) {
    if (request.skip) {
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
    if (request.skip) {
      return Q.resolve(request);
    }
    let handler = this.processor[request.type];
    if (!handler) {
      request.markSkip('Warning', `No handler found for request type: ${request.type}`);
      return request;
    }

    request.document = handler.call(this.processor, request);
    return Q.resolve(request);
  }

  _storeDocument(request) {
    // See if we should skip storing the document.  Test request.store explicitly for false as it may just not be set.
    if (request.skip || !this.store || !request.document || request.store === false) {
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
    return this.queue.done(request).then(() => { return request; });
  }

  _logOutcome(request) {
    const outcome = request.outcome ? request.outcome : 'Processed';
    if (outcome === 'Error') {
      this.logger.log(outcome, request.message);
    } else {
      request.addMeta({ total: Date.now() - request.start });
      this.logger.log('info', `${outcome} ${request.type} [${request.url}] ${request.message || ''}`, request.meta);
    }
    return request;
  }

  // ===============  Helpers  ============

  // TODO make a queue all and add promises (then) to the code below
  queueBase(request, newRequest, queue = null) {
    if (this._configFilter(newRequest.type, newRequest.url)) {
      this.logger.log('info', `Skipped queuing ${newRequest.type} [${newRequest.url}]`);
      return;
    }
    queue = queue || this.queue;
    request.promises.push(queue.push(newRequest));
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
// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const debug = require('debug')('crawler:crawler');
const fs = require('fs');
const moment = require('moment');
const Q = require('q');
const Request = require('./request');
const URL = require('url');
const uuid = require('node-uuid');
const _ = require('lodash');

debug.log = console.info.bind(console);

const defaultOptions = {
  name: 'crawler',
  count: 0,
  pollingDelay: 5000,
  processingTtl: 60 * 1000,
  promiseTrace: false,
  requeueDelay: 5000,
  deadletterPolicy: 'always' // Another option: excludeNotFound
}

class Crawler {

  constructor(queues, store, deadletters, locker, fetchers, processors, options) {
    this.queues = queues;
    this.store = store;
    this.deadletters = deadletters;
    this.locker = locker;
    this.fetchers = fetchers;
    this.processors = processors;
    this.options = this._addDefaultOptions(options);
    this.options._config.on('changed', this._reconfigure.bind(this));
    this.logger = this.options.logger;
    this.counter = 0;
    this.counterRollover = Number.parseInt('zzz', 36);
    this.deferring = false;
  }

  _addDefaultOptions(options) {
    return Object.assign(defaultOptions, options);
  }

  // Do something when the configuration controlling the crawler has changed
  _reconfigure(current, changes) {
    // the count option is consulted dynamically so no lifecycle needed here
  }

  run(context) {
    if (context.delay === -1) {
      // We are done so call the done handler and return without continuing the loop
      return context.done ? context.done() : null;
    }
    const delay = context.currentDelay;
    context.currentDelay = 0;
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
        .then(this._computeDelay.bind(this, context), this._panic.bind(this, context))
        .finally(this.run.bind(this, context));
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
    requestBox.loopName = context.name;
    return Q()
      .then(this._getRequest.bind(this, requestBox, context))
      .then(this._filter.bind(this))
      .then(this._fetch.bind(this))
      .then(this._convertToDocument.bind(this))
      .then(this._processDocument.bind(this))
      .then(this._storeDocument.bind(this))
      .catch(this._errorHandler.bind(this, requestBox))
      .then(this._completeRequest.bind(this), this._completeRequest.bind(this))
      .catch(this._errorHandler.bind(this, requestBox))
      .then(this._logOutcome.bind(this))
      .catch(this._errorHandler.bind(this, requestBox));
  }

  _errorHandler(requestBox, error) {
    error = (error instanceof Error) ? error : new Error(error);
    // if there is already a request in process, mark it as skip/requeue and carry on
    if (requestBox[0]) {
      this.logger.error(error, requestBox[0].meta);
      if (requestBox[0].type === '_errorTrap') {
        return requestBox[0];
      }
      return requestBox[0].markRequeue('Error', error.message);
    }
    // otherwise, it is early in the processing loop so no request yet.  Make up a fake one
    // so we can complete the processing loop
    this.logger.error(error);
    const request = new Request('_errorTrap', null);
    request.delay();
    request.markSkip('Skipped', error.message);
    requestBox[0] = request;
    return request;
  }

  _getRequest(requestBox, context) {
    return this._logStartEnd('getRequest', null, () => {
      const request = this._getRequestWork(requestBox);
      return request;
    });
  }

  _getRequestWork(requestBox) {
    const self = this;
    debug(`getRequestWork(${requestBox.loopName}): enter`);
    return this.queues.pop()
      .then(request => {
        if (!request) {
          request = new Request('_blank', null);
          const delay = self.options.pollingDelay || 2000;
          request.delay(delay);
          request.markSkip('Drained  ', `Waiting ${delay} milliseconds`);
          debug(`getRequestWork(${requestBox.loopName}): drained waiting ${delay} milliseconds`);
        }
        this.counter = ++this.counter % this.counterRollover;
        request.addMeta({ loopName: requestBox.loopName, cid: this.counter.toString(36) });
        requestBox[0] = request.open(self);
        debug(`getRequestWork(${requestBox.loopName}:${request.toUniqueString()}): exit (success)`);
        if (request.attemptCount) {
          return Q.delay(requestBox[0], (self.options.requeueDelay || 5000) * request.attemptCount);
        }
        return requestBox[0];
      })
      .then(self._acquireLock.bind(self));
  }


  _acquireLock(request) {
    const loopName = request.meta ? request.meta.loopName : '';
    debug(`_acquireLock(${loopName}:${request.toUniqueString()}): enter`);
    if (!request.url || !this.locker || request.requiresLock === false) {
      debug(`_acquireLock(${loopName}:${request.toUniqueString()}): exit (nothing to do)`);
      return Q(request);
    }
    const self = this;
    return Q.try(() => {
      return self.locker.lock(request.url, self.options.processingTtl || 60 * 1000);
    }).then(
      lock => {
        debug(`_acquireLock(${loopName}:${request.toUniqueString()}): exit (success)`);
        request.lock = lock;
        return request;
      },
      error => {
        // If we could not acquire a lock, requeue.  If the "error" is a normal Exceeded scenario, requeue normally
        // noting that we could not get a lock.  For any other error, requeue and capture the error for debugging.
        debug(`_acquireLock(${loopName}:${request.toUniqueString()}): exit (error)`);
        if (error.message.startsWith('Exceeded')) {
          return request.markRequeue('Collision', 'Could not lock');
        }
        this.logger.error(error, request.meta);
        return request.markRequeue('Internal Error', error.message);
      });
  }

  _releaseLock(request) {
    const loopName = request.meta ? request.meta.loopName : '';
    debug(`_releaseLock(${loopName}:${request.toUniqueString()}): enter`);
    if (!request.lock || !this.locker) {
      debug(`_releaseLock(${loopName}:${request.toUniqueString()}): exit (nothing to do)`);
      return Q(request);
    }
    const self = this;
    return Q.try(() => {
      return this.locker.unlock(request.lock);
    }).then(
      () => {
        debug(`_releaseLock(${loopName}:${request.toUniqueString()}): exit (success)`);
        request.lock = null;
        return request;
      },
      error => {
        debug(`_releaseLock(${loopName}:${request.toUniqueString()}): exit (error)`);
        request.lock = null;
        self.logger.error(error);
        return request;
      });
  }

  _completeRequest(request, forceRequeue = false) {
    // There are two paths through here, happy and sad.  The happy path requeues the request (if needed),
    // waits for all the promises to finish and then releases the lock on the URL and deletes the request
    // from the queue.  However, if requeuing fails we should still release the lock but NOT delete the
    // request from the queue (we were not able to put it back on so leave it there to expire and be
    // redelivered).  In the sad case we don't really need to wait for the promises as we are already going
    // to reprocess the request.
    // Unfortunately, this may result in a buildup of requests being processed over and over and not counted
    // (attemptCount will not be updated in the queuing system).  Since the requeue issue is likely something
    // to do with queuing in general, the theory is that the queue system's retry count will deadletter the
    // request eventually.
    //
    // Basic workflow
    // requeue
    //    if error, log, release and abandon (don't bother to wait for promises as we were requeuing any way')
    // wait for promises
    //    if error, try requeue
    //    else release
    //      if release fails abandon as everyone will think it is still in the queue
    //      else delete
    const loopName = request.meta ? request.meta.loopName : '';
    debug(`_completeRequest(${loopName}:${request.toUniqueString()}): enter (force: ${forceRequeue})`);
    const self = this;

    if (forceRequeue || (request.shouldRequeue() && request.url)) {
      return Q
        .try(() => {
          return self._requeue(request);
        })
        .catch(error => {
          debug(`_completeRequest(${loopName}:${request.toUniqueString()}): catch force requeue`);
          self.logger.error(error);
          throw error;
        })
        .finally(() => {
          return self._releaseLock(request);
        })
        .then(() => {
          return self._deleteFromQueue(request);
        }, error => {
          return self._abandonInQueue(request);
        })
        .then(() => {
          debug(`_completeRequest(${loopName}:${request.toUniqueString()}): exit (success - force requeue)`);
          return request;
        });
    }

    const trackCleanup = request.getTrackedCleanups().forEach(cleanup => cleanup());

    const trackedPromises = request.getTrackedPromises();
    let completedPromises = 0;
    let failedPromises = 0;
    for (let i = 0; i < trackedPromises.length; i++) {
      const originalPromise = trackedPromises[i];

      const loggingPromise = originalPromise.then(result => {
        completedPromises++;
        debug(`_completeRequest(${loopName}:${request.toUniqueString()}): completed ${completedPromises} of ${trackedPromises.length} promises (${failedPromises} failed)`);
        return result;
      }, error => {
        completedPromises++;
        failedPromises++;
        debug(`_completeRequest(${loopName}:${request.toUniqueString()}): completed ${completedPromises} of ${trackedPromises.length} promises (${failedPromises} failed)`);
        throw error;
      });
    }
    debug(`_completeRequest(${loopName}:${request.toUniqueString()}): ${trackedPromises.length} tracked promises`);
    const completeWork = Q.all(trackedPromises).then(
      () => {
        debug(`_completeRequest(${loopName}:${request.toUniqueString()}): resolved tracked promises`);
        return self._releaseLock(request).then(
          () => {
            return self._deleteFromQueue(request);
          },
          error => {
            debug(`_completeRequest(${loopName}:${request.toUniqueString()}): catch release lock`);
            self.logger.error(error);
            return self._abandonInQueue(request);
          });
      },
      error => {
        debug(`_completeRequest(${loopName}:${request.toUniqueString()}): catch tracked promises`);
        self.logger.error(error);
        return self._completeRequest(request, true);
      });
    return completeWork.then(() => {
      debug(`_completeRequest(${loopName}:${request.toUniqueString()}): exit (success)`);
      return request;
    }).catch(error => {
      debug(`_completeRequest(${loopName}:${request.toUniqueString()}): catch completeWork`);
      throw error;
    });
  }

  _requeue(request) {
    const loopName = request.meta ? request.meta.loopName : '';
    debug(`_requeue(${loopName}:${request.toUniqueString()}): enter`);
    return Q.try(() => {
      request.attemptCount = request.attemptCount || 0;
      if (++request.attemptCount > 5) {
        return this._storeDeadletter(request, `Exceeded attempt count for ${request.type}@${request.url}`);
      }
      request.addMeta({ attempt: request.attemptCount });
      const queuable = request.createRequeuable();
      return this.queues.repush(request, queuable);
    }).then(result => {
      debug(`_requeue(${loopName}:${request.toUniqueString()}): exit (success)`);
      return result;
    });
  }

  _filter(request) {
    const loopName = request.meta ? request.meta.loopName : '';
    debug(`_filter(${loopName}:${request.toUniqueString()}): enter`);
    if (request.shouldSkip()) {
      debug(`_filter(${loopName}:${request.toUniqueString()}): exit (nothing to do)`);
      return request;
    }
    if (!request.type || !request.url) {
      // park the malformed request in the dead queue for debugging and ignore the returned promise
      debug(`_filter(${loopName}:${request.toUniqueString()}): exit (malformed)`);
      return this._storeDeadletter(request, `Detected malformed request ${request.toString()}`);
    }
    if (this._shouldFilter(request)) {
      debug(`_filter(${loopName}:${request.toUniqueString()}): exit (success - filtered)`);
      request.markSkip('Declined');
    }
    debug(`_filter(${loopName}:${request.toUniqueString()}): exit (success - not filtered)`);
    return request;
  }

  _fetch(request) {
    const loopName = request.meta ? request.meta.loopName : '';
    debug(`_fetch(${loopName}:${request.toUniqueString()}): enter`);
    if (request.shouldSkip()) {
      debug(`_fetch(${loopName}:${request.toUniqueString()}): exit (nothing to do)`);
      return request;
    }
    if (request.payload) {
      // The request already has the document, so no need to fetch.  Setup the request as if it was actually fetched.
      request.document = request.payload.body || {};
      request.contentOrigin = 'origin';
      request.response = { headers: { etag: request.payload.etag } };
      if (request.payload.fetchedAt) {
        request.response.headers.fetchedAt = request.payload.fetchedAt;
      }
      debug(`_fetch(${loopName}:${request.toUniqueString()}): exit (success - payload)`);
      return request;
    }
    return this._logStartEnd('fetching', request, () => {
      const handler = this._getFetcher(request);
      if (!handler) {
        request.markSkip('Skip', `No fetcher found for request type: ${request.type}`);
        return request;
      }
      return handler(request);
    }).then(request => {
      debug(`_fetch(${loopName}:${request.toUniqueString()}): exit (success - fetched)`);
      return request;
    });
  }

  _getFetcher(request) {
    for (let i = 0; i < this.fetchers.length; i++) {
      const fetcher = this.fetchers[i];
      const handler = fetcher.getHandler(request);
      if (handler)
        return handler;
    }
    return null;
  }

  _convertToDocument(request) {
    const loopName = request.meta ? request.meta.loopName : '';
    debug(`_convertToDocument(${loopName}:${request.toUniqueString()}): enter`);
    if (request.shouldSkip()) {
      debug(`_convertToDocument(${loopName}:${request.toUniqueString()}): exit (nothing to do)`);
      return Q(request);
    }

    if (request.context.deletedAt) {
      if (!request.response._metadataTemplate) {
        throw new Error('Delete without metadata');
      }
      request.document._metadata = request.response._metadataTemplate;
      return Q(request);
    }

    const metadata = {
      type: request.type,
      url: request.url,
      fetchedAt: moment.utc().toISOString(),
      links: {}
    };
    if (request.response) {
      if (request.response.headers) {
        if (request.response.headers.etag) {
          metadata.etag = request.response.headers.etag;
        }
        if (request.response.headers.link) {
          metadata.headers = { link: request.response.headers.link };
        }
        if (request.response.headers.fetchedAt) {
          metadata.fetchedAt = request.response.headers.fetchedAt;
        }
      }
      // overlay any metadata that we might be carrying from a version of this doc that we already have
      if (request.response._metadataTemplate) {
        metadata.fetchedAt = request.response._metadataTemplate.fetchedAt;
        metadata.version = request.response._metadataTemplate.version;
      }
    }

    // If the doc is an array,
    // * wrap it in an object to make storage more consistent (Mongo can't store arrays directly)
    // * save the link header as GitHub will not return those in a subsequent 304
    if (Array.isArray(request.document)) {
      request.document = { elements: request.document };
    }
    if (typeof request.document === 'string')
      console.log('got a string document');
    request.document._metadata = metadata;
    debug(`_convertToDocument(${loopName}:${request.toUniqueString()}): exit (success)`);
    return Q(request);
  }

  _processDocument(request) {
    const loopName = request.meta ? request.meta.loopName : '';
    debug(`_processDocument(${loopName}:${request.toUniqueString()}): enter`);
    if (request.shouldSkip()) {
      debug(`_processDocument(${loopName}:${request.toUniqueString()}): exit (nothing to do)`);
      return Q(request);
    }
    // if the request is a delete, mark the document as deleted and be done
    if (request.context.deletedAt) {
      request.document._metadata.deletedAt = request.context.deletedAt;
      request.outcome = request.outcome || 'Deleted';
      return request;
    }
    return this._logStartEnd('processing', request, () => {
      return this._process(request).then(result => {
        debug(`_processDocument(${loopName}:${request.toUniqueString()}): exit (success)`);
        return result;
      });
    });
  }

  _process(request) {
    const handler = this._getHandler(request);
    if (!handler) {
      request.markSkip('Skip', `No handler found for request type: ${request.type}`);
      return Q(request);
    }

    const oldVersion = request.document._metadata.version;
    if (request.policy.shouldProcess(request, this.version)) {
      request.processMode = 'process';
    } else {
      // We are not going to process but may still need to traverse the doc to get to referenced docs that
      // do need processing. If so, mark the request for no saving (already have good content) and carry on.
      // Otherwise, skip the doc altogether.
      if (request.policy.shouldTraverse(request)) {
        request.processMode = 'traverse';
        request.markNoSave();
      } else {
        request.markSkip('Excluded', `Traversal policy excluded this resource`);
        return Q(request);
      }
    }

    return Q(handler(request)).then(result => {
      if (result) {
        // Be resilient to processors returning either the request or a document.
        request.document = result === request ? request.document : result;
        const metadata = request.document._metadata;
        metadata.processedAt = moment.utc().toISOString();
        if (metadata.version !== oldVersion) {
          request.markSave();
        }
      }
      if (!request.shouldSave()) {
        request.outcome = request.outcome || 'Traversed';
      }
      return request;
    });
  }


  _getHandler(request) {
    for (let i = 0; i < this.processors.length; i++) {
      const processor = this.processors[i];
      const handler = processor.getHandler(request);
      if (handler)
        return handler;
    }
    return null;
  }

  _logStartEnd(name, request, work) {
    const start = Date.now();
    let uniqueString = request ? request.toUniqueString() : '';
    const meta = request ? request.meta : null;
    this.logger.verbose(`Started ${name} ${uniqueString}`, meta);
    let result = null;
    return Q
      .try(() => { return work(); })
      .then(workResult => {
        result = workResult;
        return result;
      })
      .finally(() => {
        // in the getRequest case we did not have a request to start.  Report on the one we found.
        if (!request && result instanceof Request) {
          uniqueString = result.toUniqueString();
        } else if (uniqueString === '') {
          // This is likely a case where an error is being thrown out of the work. Let it go as it will be
          // caught and handled by the outer context.
          console.log('what?!');
        }
        this.logger.verbose(`Finished ${name} (${Date.now() - start}ms) ${uniqueString}`, meta);
      });
  }

  _storeDocument(request) {
    const loopName = request.meta ? request.meta.loopName : '';
    debug(`_storeDocument(${loopName}:${request.toUniqueString()}): enter`);
    if (request.shouldSkip() || !request.shouldSave()) {
      debug(`_storeDocument(${loopName}:${request.toUniqueString()}): exit (nothing to do)`);
      return Q(request);
    }

    const start = Date.now();
    const documentToStore = this._buildDocumentToStore(request.document);
    return this.store.upsert(documentToStore).then(upsert => {
      request.upsert = upsert;
      request.addMeta({ write: Date.now() - start });
      debug(`_storeDocument(${loopName}:${request.toUniqueString()}): exit (success)`);
      return request;
    });
  }

  _buildDocumentToStore(document) {
    const contentLocation = document._metadata.contentLocation;
    if (!contentLocation)
      // no location indicates the content is inline in the document
      return document;

    const contentType = document._metadata.contentType;
    const buffer = fs.readFileSync(contentLocation);
    // if the content is JSON, embed it, otherwise encode
    const content = contentType === 'application/json'
      ? JSON.parse(buffer)
      : buffer.toString('base64');
    // trim the content prop out as it has local machine state that is uninteresting
    const _metadata = _.omit(document._metadata, ['contentLocation']);
    return { _metadata, content };
  }

  _deleteFromQueue(request) {
    const loopName = request.meta ? request.meta.loopName : '';
    debug(`_deleteFromQueue(${loopName}:${request.toUniqueString()}): enter`);
    return Q.try(() => {
      return this.queues.done(request).then(() => {
        debug(`_deleteFromQueue(${loopName}:${request.toUniqueString()}): exit (success)`);
        return request;
      });
    });
  }

  _abandonInQueue(request) {
    const loopName = request.meta ? request.meta.loopName : '';
    debug(`_abandonInQueue(${loopName}:${request.toUniqueString()}): enter`);
    return Q.try(() => {
      return this.queues.abandon(request).then(() => {
        debug(`_abandonInQueue(${loopName}:${request.toUniqueString()}): exit (success)`);
        return request;
      });
    });
  }

  _logOutcome(request) {
    const outcome = request.outcome ? request.outcome : 'Processed';
    request.addMeta({ time: Date.now() - request.start });
    // If this request is deferred, and we've already logged that info, return.
    if (request.isDeferred() && this.deferring) {
      return request;
    }
    this.deferring = request.isDeferred();
    this.logger.info(`${outcome} ${request.type}@${request.url} ${request.message || ''}`, request.meta);
    return request;
  }

  // ===============  Helpers  ============

  _storeDeadletter(request, reason, error = null) {
    request._error = error;
    request.crawler = this;
    return request.markDead('Deadletter', reason);
  }

  storeDeadletter(request, reason = null) {
    const loopName = request.meta ? request.meta.loopName : '';
    debug(`storeDeadletter(${loopName}:${request.toUniqueString()}): enter`);
    if (this.options.deadletterPolicy === 'excludeNotFound' && reason && reason.toLowerCase().includes('status 404')) {
      this.logger.info(`storeDeadletter(${loopName}:${request.toUniqueString()}): not storing due to configured deadletter policy`);
      return Q(request);
    }
    const document = this._createDeadletter(request, reason);
    return this.deadletters.upsert(document).then(() => {
      debug(`storeDeadletter(${loopName}:${request.toUniqueString()}): exit (success)`);
      return request;
    });
  }

  _createDeadletter(request, reason) {
    const deadDocument = request.createRequeuable();
    const metadata = deadDocument._metadata = {};
    if (request._error) {
      metadata.errorMessage = request._error.message;
      metadata.errorStack = request._error.stack;
    }
    metadata.version = 1;
    metadata.meta = request.meta;
    metadata.type = 'deadletter';
    metadata.url = request.url.replace('//', '//deadletter.');
    metadata.fetchedAt = metadata.processedAt = moment.utc().toISOString();
    metadata.links = { self: { href: `urn:deadletter:${uuid.v4()}`, type: 'resource' } };
    metadata.extra = { type: request.type, url: request.url, reason: reason };
    return deadDocument;
  }

  queue(requests, name = null) {
    return this.queues.push(this._preFilter(requests), name || 'normal');
  }

  _preFilter(requests) {
    const list = Array.isArray(requests) ? requests : [requests];
    return list.filter(request => {
      if (!request.url || !request.type) {
        this._storeDeadletter(request, `Attempt to queue malformed request ${request.toString()}`);
        return false;
      }
      if (this._shouldFilter(request)) {
        this.logger.verbose(`Pre-filtered ${request.type}@${request.url}`, request.meta);
        return false;
      }
      return true;
    });
  }

  _shouldFilter(request) {
    if (!request.policy) {
      return false;
    }
    return false;
  }
}

module.exports = Crawler;

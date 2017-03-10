// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const memoryCache = require('memory-cache');
const NestedQueue = require('./nestedQueue');
const Q = require('q');
const qlimit = require('qlimit');

class AttenuatedQueue extends NestedQueue{
  constructor(queue, options) {
    super(queue);
    this.options = options;
    this.logger = options.logger;
  }

  push(requests) {
    const self = this;
    requests = Array.isArray(requests) ? requests : [requests];
    return Q.all(requests.map(qlimit(this.options.parallelPush || 1)(request => {
      return self._pushOne(request);
    })));
  }

  _pushOne(request) {
    // Include the attempt count in the key.  This allows for one concurrent requeue
    const attemptCount = request.attemptCount || 0;
    const key = `t:${attemptCount}:${request.toUniqueString()}`;
    let entry = memoryCache.get(key);
    if (entry) {
      // We've seen this request recently. The push is either in progress (and may fail) or is already done.
      // Either way, tack handlers on the (potentially) pending promise such that success is passed through
      // and rejection causes this call's request to be pushed (i.e., retry).  Ensure that the new promise
      // is stored for the next caller.  This approach attempts to eliminate the JavaScript lockstep tick
      // execution where multiple "chains" all read, then all update, then all write thereby missing the fact
      // that there are several chains writing.
      const attemptString = attemptCount ? `(attempt ${request.attemptCount}) ` : '';
      this.logger.verbose(`Attenuated ${attemptString}${request.type}@${request.url}`);
      // overwrite the promise so we keep the same ttl as the original
      entry.promise = entry.promise.catch(error => {
        return this.queue.push(request);
      });
      request.queueOutcome = 'Attenuated';
      return entry.promise;
    }
    entry = {
      timestamp: Date.now(),
      promise: this.queue.push(request)
    };
    const ttl = this.options.attenuation.ttl || 1000;
    memoryCache.put(key, entry, ttl);
    return entry.promise;
  }

  _log(message) {
    return this.logger ? this.logger.silly(message) : null;
  }
}

module.exports = AttenuatedQueue;
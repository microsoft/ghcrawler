// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const limiter = require('../limiting/inmemoryRateLimiter');
const NestedQueue = require('./nestedQueue');
const Q = require('q');
const qlimit = require('qlimit');
const debug = require('debug')('crawler:queuing:ratelimitedpushqueue');
debug.log = console.info.bind(console);

class RateLimitedPushQueue extends NestedQueue {
  constructor(queue, limiter, options) {
    super(queue);
    this.limiter = limiter;
    this.options = options;
  }

  push(requests) {
    debug('push: enter');
    const self = this;
    requests = Array.isArray(requests) ? requests : [requests];
    return Q.all(requests.map(qlimit(self.options.parallelPush || 1)(request => {
      return self._pushOne(request);
    }))).then(result => {
      debug('push: exit (success)');
      return result;
    });
  }

  _pushOne(request) {
    debug(`_pushOne(${request.toUniqueString()}: enter`);
    const deferred = Q.defer();
    const self = this;
    this.limiter(null, (error, rate) => {
      if (error) {
        debug(`_pushOne(${request.toUniqueString()}: exit (error)`);
        return deferred.reject(error);
      }
      if (rate.over) {
        return deferred.resolve(Q.delay(Math.floor((self.options.pushRateWindow || 2) * 1000 / 4)).then(() => {
          debug(`_pushOne(${request.toUniqueString()}: exit (delayed)`);
          return self._pushOne(request);
        }));
      }
      debug(`_pushOne(${request.toUniqueString()}: exit (success)`);
      deferred.resolve(self.queue.push(request));
    });
    return deferred.promise;
  }
}

module.exports = RateLimitedPushQueue;
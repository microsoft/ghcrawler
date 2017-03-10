// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const extend = require('extend');
const Q = require('q');
const Request = require('ghcrawler').request;

class InMemoryCrawlQueue {
  constructor(name, options) {
    this.name = name;
    this.queue = [];
    this.options = options;
    this.logger = options.logger;
  }

  getName() {
    return this.name;
  }

  push(requests) {
    this._incrementMetric('push');
    requests = Array.isArray(requests) ? requests : [requests];
    requests = requests.map(request => extend(true, {}, request));
    this.queue = this.queue.concat(requests);
    return Q.resolve();
  }

  subscribe() {
    return Q(null);
  }

  pop() {
    const result = this.queue.shift();
    if (!result) {
      return Q();
    }

    this._incrementMetric('pop');
    return Q.resolve(Request.adopt(result));
  }

  done() {
    this._incrementMetric('done');
    return Q(null);
  }

  // We popped but cannot process right now (e.g., no rate limit).  Stash it away and allow it to be popped later.
  defer(request) {
    this._incrementMetric('defer');
    // TODO likely need to do more here.  see the amqp10 code
    this.queue.push(request);
  }

  abandon(request) {
    this._incrementMetric('abandon');
    this.queue.unshift(request);
    return Q.resolve();
  }

  flush() {
    this.queue = [];
    return Q(this);
  }

  getInfo() {
    return Q({
      count: this.queue.length,
      metricsName: this.name
    });
  }

  _incrementMetric(operation) {
    const metrics = this.logger.metrics;
    if (metrics && metrics[this.name] && metrics[this.name][operation]) {
      metrics[this.name][operation].incr();
    }
  }
}
module.exports = InMemoryCrawlQueue;
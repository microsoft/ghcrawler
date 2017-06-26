// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const memoryCache = require('memory-cache');

class InMemoryRateLimiter {

  static create(settings) {
    const limiter = new InMemoryRateLimiter(settings);
    return limiter.get.bind(limiter);
  }

  constructor(settings) {
    this.options = settings;
  }

  get(request, callback) {
    // prefix the key as the memoryCache is shared across the process
    const key = `ratelimit:${this.options.key(request)}`;
    const incr = this.options.incr ? this.options.incr(request) : 1;
    let current = memoryCache.get(key);
    if (!current) {
      current = { count: 0 };
      // add to the cache.  Note, our window is in seconds for compatibility with the redis-rate-limiter API,
      // but the memory cache works in milliseconds
      memoryCache.put(key, current, this.options.window() * 1000);
    }
    current.count += incr;
    callback(null, {
      key: key,
      current: current.count,
      limit: this.options.limit(),
      window: this.options.window(),
      over: current.count > this.options.limit()
    });
  }
}

module.exports = InMemoryRateLimiter;
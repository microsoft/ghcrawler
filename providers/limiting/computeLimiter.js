// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const moment = require('moment');
const Q = require('q');

class ComputeLimiter {
  constructor(limiter, options) {
    this.limiter = limiter;
    this.options = options;
    this.updater = options.baselineUpdater;
    this.nextUpdate = moment();
    this.baseline = options.defaultBaseline || 500;
  }

  /**
   * Consume the given amount of resources relative to the identified key assuming that the preallocated amount has already
   * been accounted for.  If the resource limit for that key has been exceeded, exhaust the key using the supplied function.
   *
   * Also update the baseline if the given amount is lower than the current baseline.
   *
   * @return {object} An object describing what happened.  If limit is available, the object will have a 'remaining'
   * property indicating the number of resources left.  If no limit is available., the returned object has an 'overage'
   * property indicating how far over the limit you've gone, a reset property indicating when more limit will be available,
   * and an updated property indicating whether this overrun caused a change in the reset time.
   */
  consume(key, amount, preallocated, exhaust) {
    this._update();
    // in betwee updates, lower the baseline bar if we see something faster than the current baseline
    this.baseline = Math.min(amount, this.baseline);
    const consumedAmount = amount - this.baseline - preallocated;
    return this.allocate(key, consumedAmount, exhaust);
  }

  /**
   * Consume the given amount of resources relative to the identified key. If the resource limit for that key has been exceeded,
   * exhaust the key using the supplied function.
   *
   * @return {object} An object describing what happened.  If limit is available, the object will have a 'remaining'
   * property indicating the number of resources left.  If no limit is available., the returned object has an 'overage'
   * property indicating how far over the limit you've gone, a reset property indicating when more limit will be available,
   * and an updated property indicating whether this overrun caused a change in the reset time.
   */
  allocate(key, amount, exhaust) {
    const deferred = Q.defer();
    this.limiter({ key: key, amount: amount }, (error, rate) => {
      if (error) {
        return deferred.reject(error);
      }
      if (rate.over) {
        const now = Date.now();
        const resetTime = now + Math.floor(rate.window * 1000 / 4);
        const actualResetTime = exhaust(resetTime);
        const overage = rate.current - rate.limit;
        return deferred.resolve({ overage: overage, reset: actualResetTime, updated: resetTime === actualResetTime });
      }
      deferred.resolve({ remaining: rate.limit - rate.current });
    });
    return deferred.promise;
  }

  _update() {
    const now = moment();
    if (!this.updater || now.isBefore(this.nextUpdate)) {
      return;
    }
    this.nextUpdate = now.add(this.options.baselineFrequency || 60, 's');
    setTimeout(() =>
      Q
        .try(this.updater)
        .then(baseline => {
          if (baseline) {
            this.baseline = baseline;
          }
        }),
      1);
  }
}

module.exports = ComputeLimiter;
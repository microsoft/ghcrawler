// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Q = require('q');

class LimitedTokenFactory {
  constructor(factory, limiter, options) {
    this.factory = factory;
    this.limiter = limiter;
    this.logger = options.logger;
    this.options = options;
  }

  /**
   * Find all of the tokens that match the given traits and return a random one that is
   * not on the bench.  If no candidates are found, return either the soonest time one will
   * come off the bench or null if there simply were none.
   */
  getToken(traits) {
    const token = this.factory.getToken(traits);
    if (token === null || typeof token === 'number') {
      return Q(token);
    }
    const deferred = Q.defer();
    const key = token.slice(0, 4);
    this.limiter({ key: key }, (error, rate) => {
      if (error) {
        return deferred.reject(error);
      }
      if (rate.over) {
        // too many asks for this token too fast, exhaust this token for a bit to cool down.
        const now = Date.now();
        const delay = Math.floor((this.options.clientCallCapWindow || 1000) / 4);
        let restoreTime = this.exhaust(token, now + delay);
        restoreTime = restoreTime || now;
        this.logger.info('Exceeded ', `Call cap for token ${token.slice(0, 4)}. Benched until ${restoreTime - now}ms from now`);
        return deferred.resolve(restoreTime);
      }
      deferred.resolve(token);
    });
    return deferred.promise;
  }

  /**
   * Mark the given token as exhausted until the given time and return the time at which it will be restored.
   * If the token is already on the bench, it's restore time is unaffected. Null is returned if the token
   * could not be found.
   **/
  exhaust(value, until) {
    return this.factory.exhaust(value, until);
  }

  setTokens(tokens) {
    this.factory.setTokens(tokens);
  }
}

module.exports = LimitedTokenFactory;
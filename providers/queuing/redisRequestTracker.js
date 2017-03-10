// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Q = require('q');
const debug = require('debug')('crawler:queuing:requesttracker:redis');
debug.log = console.info.bind(console);

class RedisRequestTracker {
  constructor(prefix, redisClient, locker, options) {
    this.prefix = prefix;
    this.redisClient = redisClient;
    this.locker = locker;
    this.options = options;
    this.logger = options.logger;
  }

  track(request, operation) {
    debug(`track(${request.toUniqueString()}): enter`);
    const key = this._getKey(request);
    const self = this;
    return this._lockExecuteUnlock(key + '-lock', () => {
      return self._getTag(key, request).then(timestamp => {
        if (timestamp) {
          const diff = Date.now() - parseInt(timestamp, 10);
          const attemptString = request.attemptCount ? `(attempt ${request.attemptCount}) ` : '';
          self.logger.verbose(`Bounced ${attemptString}${request.type}@${request.url} from ${diff}ms ago`, request.meta);
          request.queueOutcome = 'Bounced';
          debug(`track(${request.toUniqueString()}): exit (bounced)`);
          return Q();
        }
        return operation(request).then(result => {
          debug(`track(${request.toUniqueString()}): operation success`);
          return self._setTag(key, request)
            .then(success => {
              debug(`track(${request.toUniqueString()}): exit (success)`);
              return result;
            });
        });
      });
    });
  }

  untrack(request) {
    debug(`untrack(${request.toUniqueString()}): enter`);
    const key = this._getKey(request);
    const self = this;
    return this._lockExecuteUnlock(key + '-lock', () => {
      return self._removeTag(key, request);
    }).then(request => {
      debug(`untrack(${request.toUniqueString()}): exit (success)`);
      return request;
    });
  }

  flush() {
    debug(`flush: enter`);
    // delete all the redis keys being maintained for this queue.
    const deferred = Q.defer();
    const pattern = `${this.prefix}:*`;
    const count = 10000;
    this.redisClient.eval(this._getDeleteScript(), 0, pattern, count, (error, result) => {
      if (error) {
        debug(`flush: exit (error)`);
        return deferred.reject(error);
      }
      debug(`flush: exit (success)`);
      deferred.resolve(result);
    });
    return deferred.promise;
  }

  _lockExecuteUnlock(key, operation) {
    const self = this;
    let lock = null;
    return Q
      .try(() => {
        return self.locker.lock(key, self.options.tracker.lockTtl);
      })
      .then(acquiredLock => {
        lock = acquiredLock;
        return operation();
      })
      .finally(() => {
        if (lock) {
          Q
            .try(() => {
              return self.locker.unlock(lock);
            })
            .catch(error =>
              self._log(`FAILED to unlock ${key}`));
        }
      });
  }

  _getTag(key, request) {
    const deferred = Q.defer();
    this.redisClient.get(key, function (err, reply) {
      if (err) {
        return deferred.reject(err);
      }
      deferred.resolve(reply);
    });
    return deferred.promise;
  }

  _setTag(key, request) {
    const deferred = Q.defer();
    const self = this;
    const ttl = this.options.tracker.ttl || 60000;
    this.redisClient.set([key, Date.now().toString(), 'PX', ttl, 'NX'], function (err, reply) {
      // resolve even if the set failed.  Failure to track is not fatal to the queuing operation
      // const message = err ? 'Failed to track' : 'Tracked';
      // self._log(`${message} tag: ${key}`);
      deferred.resolve(err ? null : request);
    });
    return deferred.promise;
  }

  _removeTag(key, request) {
    const deferred = Q.defer();
    const self = this;
    this.redisClient.del(key, function (err, reply) {
      if (err) {
        // This is a BIG deal. If we fail to remove here then other agents will think that everything is ok
        // and decline to queue new entries for this request when in fact, we may not be successful here.
        // Log all the info and then ensure that we fail this and cause the request to get requeued
        self.logger.error(new Error(`Failed to remove tracking tag: ${request.type}@${request.url}`), request.meta);
        return deferred.reject(err);
      }
      self._log(`Untracked tag: ${key}`);
      deferred.resolve(request);
    });
    return deferred.promise;
  }

  _getKey(request) {
    return `${this.prefix}:${request.toUniqueString()}`;
  }

  _log(message) {
    return this.logger ? this.logger.silly(message) : null;
  }

  _getDeleteScript() {
    return `
local done = false;
local cursor = "0"
redis.replicate_commands()
repeat
  local result = redis.call("SCAN", cursor, "match", ARGV[1], "count", ARGV[2])
  cursor = result[1];
  local keys = result[2];
  for i, key in ipairs(keys) do
    redis.call("DEL", key);
  end
  if cursor == "0" then
    done = true;
  end
until done
return true;
`;
  }
}
module.exports = RedisRequestTracker;
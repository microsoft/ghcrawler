// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
const redlock = require('redlock');
const redisUtil = require('./util/redis');
const factoryLogger = require('./util/logger');

function createNolock() {
  return { lock: () => null, unlock: () => { } };
}

function createLocker(options) {
  factoryLogger.info(`creating locker`, { provider: options.provider });
  if (options.provider === 'memory') {
    return createNolock();
  }
  return new redlock([redisUtil.getRedisClient(options.logger)], {
    driftFactor: 0.01,
    retryCount: options.retryCount,
    retryDelay: options.retryDelay
  });
}

exports.createLocker = createLocker;

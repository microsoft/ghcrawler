// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

let redisClient = null;
let options = null;

module.exports = defaults => {
  return run(defaults)
}

function run(defaults) {
  options = defaults;
  delete this.run;
  return this.run = getClient;
}

function getClient() {
  options.logger.info('retrieving redis client');
  if (redisClient)
    return redisClient;
  const { url, port, key, tls } = options;
  redisClient = createRedisClient(url, key, port, tls, options.logger);
  return redisClient;
}

function createRedisClient(url, key, port, tls, logger) {
  logger.info(`creating redis client`, { url: url, port: port, tls: tls });
  const options = {};
  if (key)
    options.auth_pass = key;
  if (tls)
    options.tls = { servername: url };
  // only require redis if we actually need to create a client
  const redis = require('redis');
  const result = redis.createClient(port, url, options);
  result.on('error', error => logger.info(`Redis client error: ${error}`));
  result.on('reconnecting', properties => logger.info(`Redis client reconnecting: ${JSON.stringify(properties)}`));
  setInterval(() => {
    result.ping(err => {
      if (err) {
        logger.info(`Redis client ping failure: ${err}`);
      }
    });
  }, 60 * 1000);
  return result;
}

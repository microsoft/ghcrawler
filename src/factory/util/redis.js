// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const redis = require('redis');
const config = require('painless-config');
const factoryLogger = require('./logger');

let redisClient = null;

function createRedisClient(url, key, port, tls, logger) {
  factoryLogger.info(`creating redis client`, { url: url, port: port, tls: tls });
  const options = {};
  if (key) {
    options.auth_pass = key;
  }
  if (tls) {
    options.tls = {
      servername: url
    };
  }
  const redisClient = redis.createClient(port, url, options);
  redisClient.on('error', error => logger.info(`Redis client error: ${error}`));
  redisClient.on('reconnecting', properties => logger.info(`Redis client reconnecting: ${JSON.stringify(properties)}`));
  setInterval(() => {
    redisClient.ping(err => {
      if (err) {
        logger.info(`Redis client ping failure: ${err}`);
      }
    });
  }, 60 * 1000);
  return redisClient;
}

function getRedisClient(logger) {
  factoryLogger.info('retrieving redis client');
  if (redisClient) {
    return redisClient;
  }
  const url = config.get('CRAWLER_REDIS_URL');
  const port = config.get('CRAWLER_REDIS_PORT');
  const key = config.get('CRAWLER_REDIS_ACCESS_KEY');
  const tls = config.get('CRAWLER_REDIS_TLS') === 'true';
  redisClient = createRedisClient(url, key, port, tls, logger);
  return redisClient;
}



exports.getRedisClient = getRedisClient;
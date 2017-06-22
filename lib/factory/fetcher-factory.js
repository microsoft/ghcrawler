const config = require('painless-config');
const GitHubFetcher = require('../../providers/fetcher/githubFetcher');
const factoryLogger = require('./util/logger');
const redisUtil = require('./util/redis');
const RedisRateLimiter = require('redis-rate-limiter');
const InMemoryRateLimiter = require('../../providers/limiting/inmemoryRateLimiter');
const LimitedTokenFactory = require('../../providers/fetcher/limitedTokenFactory');
const requestor = require('ghrequestor');
const ip = require('ip');
const TokenFactory = require('../../providers/fetcher/tokenFactory');
const ComputeLimiter = require('../../providers/limiting/computeLimiter');
const Q = require('q');
const request = require('request');

function createRequestor() {
  factoryLogger.info('create requestor');
  return requestor.defaults({
    // turn off the requestor's throttle management mechanism in favor of ours
    forbiddenDelay: 0,
    delayOnThrottle: false
  });
}

function createRedisComputeLimiter(redisClient, options) {
  const address = ip.address().toString();
  factoryLogger.info('create redis compute limiter', { address: address, computeWindow: options.computeWindow, computeLimit: options.computeLimit });
  return RedisRateLimiter.create({
    redis: redisClient,
    key: request => `${address}:compute:${request.key}`,
    incr: request => request.amount,
    window: () => options.computeWindow || 15,
    limit: () => options.computeLimit || 15000
  });
}

function createInMemoryComputeLimiter(options) {
  factoryLogger.info('create in memory compute limiter', { computeWindow: options.computeWindow, computeLimit: options.computeLimit });
  return InMemoryRateLimiter.create({
    key: request => 'compute:' + request.key,
    incr: request => request.amount,
    window: () => options.computeWindow || 15,
    limit: () => options.computeLimit || 15000
  });
}

function createRedisTokenLimiter(redisClient, options) {
  factoryLogger.info('create redis token limiter', { callCapWindow: options.callCapWindow, callCapLimit: options.callCapLimit });
  const ip = '';
  return RedisRateLimiter.create({
    redis: redisClient,
    key: request => `${ip}:token:${request.key}`,
    window: () => options.callCapWindow || 1,
    limit: () => options.callCapLimit
  });
}

function createInMemoryTokenLimiter(options) {
  factoryLogger.info('create in memory token limiter', { callCapWindow: options.callCapWindow, callCapLimit: options.callCapLimit });
  return InMemoryRateLimiter.create({
    key: request => 'token:' + request.key,
    window: () => options.callCapWindow || 1,
    limit: () => options.callCapLimit
  });
}

function createTokenLimiter(options) {
  factoryLogger.info('create token limiter', { capStore: options.capStore });
  return options.capStore === 'redis'
    ? createRedisTokenLimiter(redisUtil.getRedisClient(options.logger), options)
    : createInMemoryTokenLimiter(options);
}

function createTokenFactory(options) {
  factoryLogger.info('create token factory');
  const factory = new TokenFactory(config.get('CRAWLER_GITHUB_TOKENS'), options);
  const limiter = createTokenLimiter(options);
  return new LimitedTokenFactory(factory, limiter, options);
}

function networkBaselineUpdater(logger) {
  return Q.allSettled([0, 1, 2, 3].map(number => {
    return Q.delay(number * 50).then(() => {
      const deferred = Q.defer();
      request({
        url: 'https://api.github.com/rate_limit',
        headers: {
          'User-Agent': 'ghrequestor'
        },
        time: true
      }, (error, response, body) => {
        if (error) {
          return deferred.reject(error);
        }
        deferred.resolve(response.elapsedTime);
      });
      return deferred.promise;
    });
  })).then(times => {
    let total = 0;
    let count = 0;
    for (let index in times) {
      if (times[index].state === 'fulfilled') {
        total += times[index].value;
        count++;
      }
    }
    const result = Math.floor(total / count);
    logger.info(`New GitHub request baseline: ${result}`);
    return result;
  });
}

function createComputeLimiter(options) {
  factoryLogger.info('create compute limiter', { computeLimitStore: options.computeLimitStore });
  const limiter = options.computeLimitStore === 'redis'
    ? createRedisComputeLimiter(redisUtil.getRedisClient(options.logger), options)
    : createInMemoryComputeLimiter(options);
  options.baselineUpdater = networkBaselineUpdater.bind(null, options.logger);
  return new ComputeLimiter(limiter, options);
}

function createGitHubFetcher(store, options) {
  factoryLogger.info('create github fetcher');
  const requestor = createRequestor();
  const tokenFactory = createTokenFactory(options);
  const limiter = createComputeLimiter(options);
  return new GitHubFetcher(requestor, store, tokenFactory, limiter, options);
}

exports.createGitHubFetcher = createGitHubFetcher;
const RedisMetrics = require('redis-metrics');
const factoryLogger = require('./util/logger');
const redisUtil = require('./util/redis');

function createQueuingMetrics(crawlerName, options) {
    if (options.metricsStore !== 'redis') {
      return null;
    }
    const metrics = new RedisMetrics({ client: redisUtil.getRedisClient(options.logger) });
    const queueNames = ['immediate', 'soon', 'normal', 'later', 'events'];
    const operations = ['push', 'repush', 'done', 'abandon'];
    const queuesMetrics = {};
    const queueNamePrefix = options.queueName;
    queueNames.forEach(queueName => {
      queuesMetrics[queueName] = {};
      operations.forEach(operation => {
        const name = `${queueNamePrefix}:${queueName}:${operation}`;
        queuesMetrics[queueName][operation] = metrics.counter(name, { timeGranularity: 'second', namespace: 'crawlermetrics' }); // Stored in Redis as {namespace}:{name}:{period}
      });
    });
    return queuesMetrics;
  }

  function createFetcherMetrics(crawlerName, options) {
    factoryLogger.info('create fetcher metrics', { metricsStore: options.metricsStore });
    if (options.metricsStore !== 'redis') {
      return null;
    }
    const metrics = new RedisMetrics({ client: redisUtil.getRedisClient(options.logger) });
    const names = ['fetch'];
    const result = {};
    names.forEach(name => {
      const fullName = `${crawlerName}:github:${name}`;
      result[name] = metrics.counter(fullName, { timeGranularity: 'second', namespace: 'crawlermetrics' }); // Stored in Redis as {namespace}:{name}:{period}
    });
    return result;
  }

  exports.createQueuingMetrics = createQueuingMetrics;
  exports.createFetcherMetrics = createFetcherMetrics;
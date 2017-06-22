const config = require('painless-config');
const factoryLogger = require('./util/logger');
const Q = require('q');
const CrawlerService = require('../crawlerService');
const getDefaultOptions = require('../options').getDefaultOptions;
const RefreshingConfig = require('refreshing-config');
const RefreshingConfigRedis = require('refreshing-config-redis');
const createLogger = require('../logging').createLogger;
const redisUtil = require('./util/redis');
const crawlerFactory = require('./crawler-factory');

const subsystemNames = ['crawler', 'fetcher', 'queuing', 'storage', 'locker'];

function createRedisRefreshingConfig(crawlerName, subsystemName) {
  factoryLogger.info('Create refreshing redis config', { crawlerName: crawlerName, subsystemName: subsystemName });
  const redisClient = redisUtil.getRedisClient(createLogger(true));
  const key = `${crawlerName}:options:${subsystemName}`;
  const channel = `${key}-channel`;
  const configStore = new RefreshingConfigRedis.RedisConfigStore(redisClient, key);
  const config = new RefreshingConfig.RefreshingConfig(configStore)
    .withExtension(new RefreshingConfigRedis.RedisPubSubRefreshPolicyAndChangePublisher(redisClient, channel));
  return config;
}

function createInMemoryRefreshingConfig(values = {}) {
  factoryLogger.info('create in memory refreshing config');
  const configStore = new RefreshingConfig.InMemoryConfigStore(values);
  const config = new RefreshingConfig.RefreshingConfig(configStore)
    .withExtension(new RefreshingConfig.InMemoryPubSubRefreshPolicyAndChangePublisher());
  return config;
}

function initializeSubsystemOptions(config, defaults) {
  if (Object.getOwnPropertyNames(config).length > 1) {
    return Q(config);
  }
  return Q.all(Object.getOwnPropertyNames(defaults).map(optionName => {
    return config._config.set(optionName, defaults[optionName]);
  })).then(() => { return config._config.getAll(); });
}

function createRefreshingOptions(crawlerName, subsystemNames, provider = 'redis') {
  factoryLogger.info(`creating refreshing options with crawlerName:${crawlerName}`);
  const result = {};
  provider = provider.toLowerCase();
  return Q.all(subsystemNames.map(subsystemName => {
    factoryLogger.info(`creating refreshing options promise with crawlerName:${crawlerName} subsystemName ${subsystemName} provider ${provider}`);
    let config = null;
    if (provider === 'redis') {
      config = createRedisRefreshingConfig(crawlerName, subsystemName);
    } else if (provider === 'memory') {
      config = createInMemoryRefreshingConfig();
    } else {
      throw new Error(`Invalid options provider setting ${provider}`);
    }
    return config.getAll().then(values => {
      factoryLogger.info(`creating refreshingOption config get completed`);
      const defaults = getDefaultOptions();
      return initializeSubsystemOptions(values, defaults[subsystemName]).then(resolved => {
        factoryLogger.info(`subsystem options initialized`);
        result[subsystemName] = values;
      });
    });
  })).then(() => { return result; });
}

function createService(name) {
  factoryLogger.info('appInitStart');
  const crawlerName = config.get('CRAWLER_NAME') || 'crawler';
  const optionsProvider = name === 'InMemory' ? 'memory' : (config.get('CRAWLER_OPTIONS_PROVIDER') || 'memory');

  const crawlerPromise = createRefreshingOptions(crawlerName, subsystemNames, optionsProvider).then(options => {
    factoryLogger.info(`creating refreshingOption completed`);
    name = name || 'InMemory';
    factoryLogger.info(`begin create crawler of type ${name}`);
    const crawler = crawlerFactory[`create${name}Crawler`](options);
    return [crawler, options];
  });
  return new CrawlerService(crawlerPromise);
}

exports.createService = createService;
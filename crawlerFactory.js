// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const RedisRequestTracker = require('./providers/queuing/redisRequestTracker');
const DeltaStore = require('./providers/storage/deltaStore');
const MongoDocStore = require('./providers/storage/mongodocstore');
const UrlToUrnMappingStore = require('./providers/storage/urlToUrnMappingStore');
const AzureTableMappingStore = require('./providers/storage/tableMappingStore');
const AzureStorage = require('azure-storage');
const config = require('painless-config');
const Crawler = require('./lib/crawler');
const CrawlerService = require('./lib/crawlerService');
const Q = require('q');
const QueueSet = require('./providers/queuing/queueSet');
const redlock = require('redlock');
const RefreshingConfig = require('@microsoft/refreshing-config');
const RefreshingConfigRedis = require('refreshing-config-redis');

let logger = null;
let providerSearchPath = null;
let finalOptions = null;

class CrawlerFactory {
  static createService(defaults, appLogger, searchPath = []) {
    logger = appLogger;
    logger.info('appInitStart');
    providerSearchPath = [require('./providers')];
    // initialize the redis provider (if any) ASAP since it is used all over and we want to share the client
    CrawlerFactory._initializeRedis(defaults);

    const optionsProvider = defaults.provider || 'memory';
    const crawlerName = (defaults.crawler && defaults.crawler.name) || 'crawler';
    searchPath.forEach(entry => providerSearchPath.push(entry));
    const subsystemNames = ['crawler', 'filter', 'fetch', 'process', 'queue', 'store', 'deadletter', 'lock'];
    const crawlerPromise = CrawlerFactory.createRefreshingOptions(crawlerName, subsystemNames, defaults, optionsProvider).then(options => {
      logger.info(`created all refreshingOptions`);
      finalOptions = options;
      const crawler = CrawlerFactory.createCrawler(options);
      return [crawler, options];
    });
    return new CrawlerService(crawlerPromise);
  }

  static _initializeRedis(defaults) {
    if (defaults.redis && defaults.redis.provider)
      CrawlerFactory._getProvider(defaults.redis || {}, defaults.redis.provider, 'redis');
  }

  static _decorateOptions(key, options) {
    if (!options.logger) options.logger = logger;
    if (!options.logger.metrics) {
      const capitalized = key.charAt(0).toUpperCase() + key.slice(1);
      const metricsFactory = CrawlerFactory[`create${capitalized}Metrics`];
      if (metricsFactory) {
        logger.info('Creating metrics factory', {factory: capitalized});
        logger.metrics = metricsFactory(options.crawler.name, options[key]);
      }
    }
  }

  static createCrawler(options, { queues = null, store = null, deadletters = null, locker = null, filter = null, fetchers = null, processors = null } = {}) {
    logger.info('creating crawler');
    queues = queues || CrawlerFactory.createQueues(options.queue);
    if (options.event)
      CrawlerFactory.addEventQueue(queues, options.event);
    store = store || CrawlerFactory.createStore(options.store);
    deadletters = deadletters || CrawlerFactory.createDeadLetterStore(options.deadletter);
    locker = locker || CrawlerFactory.createLocker(options.lock);
    processors = processors || CrawlerFactory.createProcessors(options.process);
    filter = filter || CrawlerFactory.createFilter(options.filter, processors);
    fetchers = fetchers || CrawlerFactory.createFetchers(options.fetch, store, processors, filter);
    // The crawler is not "provided" so ensure the options are decorated as necessary (e.g., logger)
    CrawlerFactory._decorateOptions('crawler', options.crawler);
    const result = new Crawler(queues, store, deadletters, locker, fetchers, processors, options.crawler);
    result.initialize = CrawlerFactory._initialize.bind(result);
    return result;
  }

  static _initialize() {
    return Q.try(this.queues.subscribe.bind(this.queues))
      .then(this.store.connect.bind(this.store))
      .then(this.deadletters.connect.bind(this.deadletters));
  }

  static createRefreshingOptions(crawlerName, subsystemNames, defaults, refreshingProvider = 'redis') {
    logger.info(`creating refreshing options with crawlerName:${crawlerName}`);
    const result = {};
    refreshingProvider = refreshingProvider.toLowerCase();
    return Q.all(subsystemNames.map(subsystemName => {
      // Any given subsytem may have a provider or may be a list of providers. If a particular provider is
      // identified then hook up just that set of options for refreshing.
      logger.info(`creating refreshing options ${subsystemName} with provider ${refreshingProvider}`);
      let config = null;
      const subDefaults = defaults[subsystemName] || {};
      const subProvider = subDefaults && subDefaults.provider;
      const uniqueName = `${subsystemName}${subProvider ? '-' + subProvider : ''}`;
      if (refreshingProvider === 'redis') {
        config = CrawlerFactory.createRedisRefreshingConfig(crawlerName, uniqueName);
      } else if (refreshingProvider === 'memory') {
        config = CrawlerFactory.createInMemoryRefreshingConfig();
      } else {
        throw new Error(`Invalid refreshing provider setting ${refreshingProvider}`);
      }
      return config.getAll().then(values => {
        logger.info(`got refreshingOption values for ${subsystemName}`);
        // grab the right defaults. May need to drill down a level if the subsystem has a provider
        const trueDefaults = subProvider ? subDefaults[subProvider] || {} : subDefaults;
        return CrawlerFactory.initializeSubsystemOptions(values, trueDefaults).then(resolved => {
          logger.info(`${subsystemName} options initialized`);
          // Hook the refreshing options into the right place in the result structure.
          // Be sure to retain the 'provider' setting
          if (subProvider)
            result[subsystemName] = { provider: subProvider, [subProvider]: values };
          else
            result[subsystemName] = values;
        });
      });
    })).then(() => { return result; });
  }

  static initializeSubsystemOptions(config, defaults) {
    if (Object.getOwnPropertyNames(config).length > 1) {
      return Q(config);
    }
    return Q.all(Object.getOwnPropertyNames(defaults).map(optionName => {
      return config._config.set(optionName, defaults[optionName]);
    })).then(() => { return config._config.getAll(); });
  }

  static createRedisRefreshingConfig(crawlerName, subsystemName) {
    logger.info('Creating refreshing redis config', {
      crawlerName: crawlerName,
      subsystemName: subsystemName
    });
    const redisClient = CrawlerFactory.getProvider('redis');
    const key = `${crawlerName}:options:${subsystemName}`;
    const channel = `${key}-channel`;
    const configStore = new RefreshingConfigRedis.RedisConfigStore(redisClient, key);
    const config = new RefreshingConfig.RefreshingConfig(configStore)
      .withExtension(new RefreshingConfigRedis.RedisPubSubRefreshPolicyAndChangePublisher(redisClient, channel));
    return config;
  }

  static createInMemoryRefreshingConfig(values = {}) {
    logger.info('creating in memory refreshing config');
    const configStore = new RefreshingConfig.InMemoryConfigStore(values);
    const config = new RefreshingConfig.RefreshingConfig(configStore)
      .withExtension(new RefreshingConfig.InMemoryPubSubRefreshPolicyAndChangePublisher());
    return config;
  }

  static getProvider(namespace, ...params) {
    const provider = finalOptions[namespace];
    if (!provider)
      return null;
    for (let i = 0; i < providerSearchPath.length; i++) {
      const entry = providerSearchPath[i];
      const result = entry[namespace] && entry[namespace][provider];
      if (result)
        return result(...params);
    }
    return require(provider)(...params);
  }

  static _getProvider(options, provider, namespace, ...params) {
    const subOptions = options[provider] || {};
    CrawlerFactory._decorateOptions(namespace, subOptions);
    subOptions.logger.info(`creating ${namespace}:${provider}`);
    for (let i = 0; i < providerSearchPath.length; i++) {
      const entry = providerSearchPath[i];
      const result = entry[namespace] && entry[namespace][provider];
      if (result)
        return result(subOptions, ...params);
    }
    return require(provider)(subOptions, ...params);
  }

  static _getAllProviders(options, namespace, ...params) {
    return CrawlerFactory._getNamedProviders(options, namespace, Object.getOwnPropertyNames(options), ...params);
  }

  static _getNamedProviders(options, namespace, names, ...params) {
    return names
      .filter(key => !['_config', 'logger', 'dispatcher', options.dispatcher].includes(key))
      .map(name =>
        CrawlerFactory._getProvider(options, name, namespace, ...params));
  }

  static createFilter(options, processors) {
    return CrawlerFactory._getProvider(options, options.provider, 'filter', processors);
  }

  static createStore(options, provider = options.provider) {
    if (provider)
      return CrawlerFactory._getProvider(options, provider, 'store');
    const names = options.dispatcher.split('+');
    const stores = CrawlerFactory._getNamedProviders(options, 'store', names.slice(1));
    const dispatcher = names[0];
    return dispatcher
      ? CrawlerFactory._getProvider(options, dispatcher, 'store', stores)
      : stores;
  }

  static createDeadLetterStore(options, provider = options.provider) {
    return CrawlerFactory._getProvider(options, provider, 'store');
  }

  static createFetchers(options, store, processors, filter) {
    const fetchers = CrawlerFactory._getAllProviders(options, 'fetch', store, processors, filter);
    return options.dispatcher
      ? [CrawlerFactory._getProvider(options, options.dispatcher, 'fetch', store, fetchers, processors, filter)]
      : fetchers;
  }

  static createProcessors(options) {
    const processors = CrawlerFactory._getAllProviders(options, 'process');
    return options.dispatcher
      ? [CrawlerFactory._getProvider(options, options.dispatcher, 'process', processors)]
      : processors;
  }

  // static createStoreOld(options) {
  //   const provider = options.provider || 'azure';
  //   factoryLogger.info(`Create store for provider ${options.provider}`);
  //   let store = null;
  //   switch (options.provider) {
  //     case 'azure': {
  //       store = CrawlerFactory.createTableAndStorageStore(options);
  //       break;
  //     }
  //     case 'azure-redis': {
  //       store = CrawlerFactory.createRedisAndStorageStore(options);
  //       break;
  //     }
  //     case 'mongo': {
  //       store = CrawlerFactory.createMongoStore(options);
  //       break;
  //     }
  //     case 'memory': {
  //       store = new InMemoryDocStore(true);
  //       break;
  //     }
  //     default: throw new Error(`Invalid store provider: ${provider}`);
  //   }
  //   store = CrawlerFactory.createDeltaStore(store, options);
  //   return store;
  // }

  static createMongoStore(options) {
    return new MongoDocStore(config.get('CRAWLER_MONGO_URL'), options);
  }

  static createRedisAndStorageStore(options, name = null) {
    logger.info(`creating azure redis store`, {name: name});
    const baseStore = CrawlerFactory.createAzureStorageStore(options, name);
    return new UrlToUrnMappingStore(baseStore, CrawlerFactory.getProvider('redis'), baseStore.name, options);
  }

  static createTableAndStorageStore(options, name = null) {
    logger.info(`creating azure store`, {name: name});
    const baseStore = CrawlerFactory.createAzureStorageStore(options, name);
    const account = config.get('CRAWLER_STORAGE_ACCOUNT');
    const key = config.get('CRAWLER_STORAGE_KEY');
    return new AzureTableMappingStore(baseStore, CrawlerFactory.createTableService(account, key), baseStore.name, options);
  }

  // static createDeadLetterStore_old(options) {
  //   const provider = options.provider || 'azure';
  //   factoryLogger.info(`Create deadletter store for provider ${options.provider}`);
  //   switch (options.provider) {
  //     case 'azure':
  //     case 'azure-redis': {
  //       return CrawlerFactory.createAzureStorageStore(options, config.get('CRAWLER_STORAGE_NAME') + '-deadletter');
  //     }
  //     case 'mongo': {
  //       return CrawlerFactory.createMongoStore(options);
  //     }
  //     case 'memory': {
  //       return new InMemoryDocStore(true);
  //     }
  //     default: throw new Error(`Invalid store provider: ${provider}`);
  //   }
  // }

  static createDeltaStore(inner, options) {
    if (!options.delta || !options.delta.provider || options.delta.provider === 'none') {
      return inner;
    }
    logger.info(`creating delta store`);
    const deltaStoreProviders = typeof options.delta.provider === 'string' ? [options.delta.provider] : options.delta.provider;
    let store = inner;
    deltaStoreProviders.forEach(deltaProvider => {
      switch (deltaProvider) {
        case 'azure':
        case 'azure-redis':
          store = CrawlerFactory.createAzureDeltaStore(store, null, options);
          break;
        default:
          try {
            const PluggableDeltaStore = require(`ghcrawler-${deltaProvider}`);
            store = new PluggableDeltaStore(store, options);
          } catch (error) {
            logger.error(error);
            throw new Error(`Invalid delta store provider: ${deltaProvider}`);
          }
      }
    });
    return store;
  }

  static createAzureDeltaStore(inner, name = null, options = {}) {
    name = name || config.get('CRAWLER_DELTA_STORAGE_NAME') || `${config.get('CRAWLER_STORAGE_NAME')}-log`;
    const account = config.get('CRAWLER_DELTA_STORAGE_ACCOUNT') || config.get('CRAWLER_STORAGE_ACCOUNT');
    const key = config.get('CRAWLER_DELTA_STORAGE_KEY') || config.get('CRAWLER_STORAGE_KEY');
    logger.info('creating delta store', {name: name, account: account});
    const retryOperations = new AzureStorage.ExponentialRetryPolicyFilter();
    const blobService = AzureStorage.createBlobService(account, key).withFilter(retryOperations);
    return new DeltaStore(inner, blobService, name, options);
  }

  static createTableService(account, key) {
    logger.info(`creating table service`);
    const retryOperations = new AzureStorage.ExponentialRetryPolicyFilter();
    return AzureStorage.createTableService(account, key).withFilter(retryOperations);
  }

  static createLocker(options, provider = options.provider || 'memory') {
    return CrawlerFactory._getProvider(options, provider, 'lock');
  }

  static createRequestTracker(prefix, options) {
    let locker = null;
    if (options.tracker && options.tracker.locking) {
      locker = new redlock([CrawlerFactory.getProvider('redis')], options.tracker);
    } else {
      locker = CrawlerFactory.createNolock();
    }
    return new RedisRequestTracker(prefix, CrawlerFactory.getProvider('redis'), locker, options);
  }

  static createNolock() {
    return { lock: () => null, unlock: () => { } };
  }

  static createQueues(options, provider = options.provider) {
    return CrawlerFactory._getProvider(options, provider, 'queue');
  }

  static createEventQueue(manager, options = {}, provider = options.provider) {
    return CrawlerFactory._getProvider(options, provider, 'events');
  }

  static createQueueSet(manager, tracker, options) {
    const immediate = manager.createQueueChain('immediate', tracker, options);
    const soon = manager.createQueueChain('soon', tracker, options);
    const normal = manager.createQueueChain('normal', tracker, options);
    const later = manager.createQueueChain('later', tracker, options);
    return new QueueSet([immediate, soon, normal, later], options);
  }

  static addEventQueue(queues, options, provider = options.provider) {
    if (provider && provider !== 'none') {
      const eventQueue = CrawlerFactory._getProvider(options, provider, 'event');
      queues.addQueue(eventQueue);
    }
  }
}

module.exports = CrawlerFactory;

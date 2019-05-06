// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const mockInsights = require('../providers/logger/mockInsights');
const LimitedTokenFactory = require('../providers/fetcher/limitedTokenFactory');
const TokenFactory = require('../providers/fetcher/tokenFactory');
const ComputeLimiter = require('../providers/limiting/computeLimiter');
const InMemoryRateLimiter = require('../providers/limiting/inmemoryRateLimiter');
const Amqp10Queue = require('../providers/queuing/amqp10Queue');
const AttenuatedQueue = require('../providers/queuing/attenuatedQueue');
const InMemoryCrawlQueue = require('../providers/queuing/inmemorycrawlqueue');
const RabbitQueueManager = require('../providers/queuing/rabbitQueueManager');
const RedisRequestTracker = require('../providers/queuing/redisRequestTracker');
const ServiceBusQueueManager = require('../providers/queuing/serviceBusQueueManager');
const InMemoryDocStore = require('../providers/storage/inmemoryDocStore');
const DeltaStore = require('../providers/storage/deltaStore');
const MongoDocStore = require('../providers/storage/mongodocstore');
const AzureStorageDocStore = require('../providers/storage/storageDocStore');
const GoogleCloudStorage = require('../providers/storage/googleCloudStorage');
const GoogleCloudDeltaStorage = require('../providers/storage/googleCloudDeltaStorage');
const UrlToUrnMappingStore = require('../providers/storage/urlToUrnMappingStore');
const AzureTableMappingStore = require('../providers/storage/tableMappingStore');
const amqp10 = require('amqp10');
const appInsights = require('applicationinsights');
const aiLogger = require('winston-azure-application-insights').AzureApplicationInsightsLogger;
const AzureStorage = require('azure-storage');
const GoogleStorage = require('@google-cloud/storage');
const config = require('painless-config');
const Crawler = require('../lib/crawler');
const CrawlerService = require('../lib/crawlerService');
const fs = require('fs');
const GitHubFetcher = require('../providers/fetcher/githubFetcher');
const GitHubProcessor = require('../providers/fetcher/githubProcessor');
const ip = require('ip');
const moment = require('moment');
const policy = require('../lib/traversalPolicy');
const Q = require('q');
const QueueSet = require('../providers/queuing/queueSet');
const redis = require('redis');
const RedisMetrics = require('redis-metrics');
const RedisRateLimiter = require('redis-rate-limiter');
const redlock = require('redlock');
const RefreshingConfig = require('@microsoft/refreshing-config');
const RefreshingConfigRedis = require('refreshing-config-redis');
const request = require('request');
const Request = require('../lib/request');
const requestor = require('ghrequestor');
const winston = require('winston');

const AmqpClient = amqp10.Client;
const AmqpPolicy = amqp10.Policy;

let factoryLogger = null;
let redisClient = null;

class CrawlerFactory {

  static getDefaultOptions() {
    return {
      crawler: {
        name: config.get('CRAWLER_NAME') || 'crawler',
        count: 0,
        pollingDelay: 5000,
        processingTtl: 60 * 1000,
        promiseTrace: false,
        requeueDelay: 5000,
        orgList: CrawlerFactory.loadOrgs(),
        deadletterPolicy: 'always' // Another option: excludeNotFound
      },
      fetcher: {
        tokenLowerBound: 50,
        metricsStore: 'redis',
        callCapStore: 'memory',
        callCapWindow: 1,       // seconds
        callCapLimit: 30,       // calls
        computeLimitStore: 'memory',
        computeWindow: 15,      // seconds
        computeLimit: 15000,    // milliseconds
        baselineFrequency: 60,  // seconds
        deferDelay: 500
      },
      queuing: {
        provider: config.get('CRAWLER_QUEUE_PROVIDER') || 'amqp10',
        queueName: config.get('CRAWLER_QUEUE_PREFIX') || 'crawler',
        credit: 100,
        weights: { events: 10, immediate: 3, soon: 2, normal: 3, later: 2 },
        messageSize: 240,
        parallelPush: 10,
        pushRateLimit: 200,
        metricsStore: 'redis',
        events: {
          provider: config.get('CRAWLER_EVENT_PROVIDER') || 'webhook',
          topic: config.get('CRAWLER_EVENT_TOPIC_NAME') || 'crawler',
          queueName: config.get('CRAWLER_EVENT_QUEUE_NAME') || 'crawler'
        },
        attenuation: {
          ttl: 3000
        },
        tracker: {
          // driftFactor: 0.01,
          // retryCount: 3,
          // retryDelay: 200,
          // locking: true,
          // lockTtl: 1000,
          ttl: 60 * 60 * 1000
        }
      },
      storage: {
        ttl: 3 * 1000,
        provider: config.get('CRAWLER_STORE_PROVIDER') || 'azure',
        delta: {
          provider: config.get('CRAWLER_DELTA_PROVIDER')
        }
      },
      locker: {
        provider: 'redis',
        retryCount: 3,
        retryDelay: 200
      }
    };
  }

  static createService(name) {
    factoryLogger.info('appInitStart');
    const crawlerName = config.get('CRAWLER_NAME') || 'crawler';
    const optionsProvider = name === 'InMemory' ? 'memory' : (config.get('CRAWLER_OPTIONS_PROVIDER') || 'memory');
    const subsystemNames = ['crawler', 'fetcher', 'queuing', 'storage', 'locker'];
    const crawlerPromise = CrawlerFactory.createRefreshingOptions(crawlerName, subsystemNames, optionsProvider).then(options => {
      factoryLogger.info(`creating refreshingOption completed`);
      name = name || 'InMemory';
      factoryLogger.info(`begin create crawler of type ${name}`);
      const crawler = CrawlerFactory[`create${name}Crawler`](options);
      return [crawler, options];
    });
    return new CrawlerService(crawlerPromise);
  }

  static createStandardCrawler(options) {
    factoryLogger.info(`creating standard Crawler Started`);
    return CrawlerFactory.createCrawler(options);
  }

  static createInMemoryCrawler(options) {
    CrawlerFactory._configureInMemoryOptions(options);
    return CrawlerFactory.createCrawler(options);
  }

  static _configureInMemoryOptions(options) {
    factoryLogger.info(`create in memory options`);
    options.crawler.count = 1;
    options.fetcher.computeLimitStore = 'memory';
    options.fetcher.metricsStore = null;
    delete options.queuing.events.provider;
    options.queuing.provider = 'memory';
    options.queuing.metricsStore = null;
    options.locker.provider = 'memory';
    options.storage.provider = 'memory';
    options.storage.delta.provider = 'none';
    return options;
  }

  static _decorateOptions(options) {
    Object.getOwnPropertyNames(options).forEach(key => {
      const logger = CrawlerFactory.createLogger(true);
      options[key].logger = logger;
      const capitalized = key.charAt(0).toUpperCase() + key.slice(1);
      const metricsFactory = CrawlerFactory[`create${capitalized}Metrics`];
      if (metricsFactory) {
        factoryLogger.info('Creating metrics factory', { factory: capitalized });
        logger.metrics = metricsFactory(options.crawler.name, options[key]);
      }
    });
  }

  static createCrawler(options, { queues = null, store = null, deadletters = null, locker = null, fetcher = null, processor = null } = {}) {
    CrawlerFactory._decorateOptions(options);
    queues = queues || CrawlerFactory.createQueues(options.queuing);
    store = store || CrawlerFactory.createStore(options.storage);
    deadletters = deadletters || CrawlerFactory.createDeadletterStore(options.storage);
    locker = locker || CrawlerFactory.createLocker(options.locker);
    fetcher = fetcher || CrawlerFactory.createGitHubFetcher(store, options.fetcher);
    processor = processor || new GitHubProcessor(store);
    const result = new Crawler(queues, store, deadletters, locker, fetcher, processor, options.crawler);
    result.initialize = CrawlerFactory._initialize.bind(result);
    return result;
  }

  static _initialize() {
    return Q.try(this.queues.subscribe.bind(this.queues))
      .then(this.store.connect.bind(this.store))
      .then(this.deadletters.connect.bind(this.deadletters));
  }

  static createRefreshingOptions(crawlerName, subsystemNames, provider = 'redis') {
    factoryLogger.info(`creating refreshing options with crawlerName:${crawlerName}`);
    const result = {};
    provider = provider.toLowerCase();
    return Q.all(subsystemNames.map(subsystemName => {
      factoryLogger.info(`creating refreshing options promise with crawlerName:${crawlerName} subsystemName ${subsystemName} provider ${provider}`);
      let config = null;
      if (provider === 'redis') {
        config = CrawlerFactory.createRedisRefreshingConfig(crawlerName, subsystemName);
      } else if (provider === 'memory') {
        config = CrawlerFactory.createInMemoryRefreshingConfig();
      } else {
        throw new Error(`Invalid options provider setting ${provider}`);
      }
      return config.getAll().then(values => {
        factoryLogger.info(`creating refreshingOption config get completed`);
        const defaults = CrawlerFactory.getDefaultOptions();
        return CrawlerFactory.initializeSubsystemOptions(values, defaults[subsystemName]).then(resolved => {
          factoryLogger.info(`subsystem '${subsystemName}' options initialized`);
          result[subsystemName] = values;
        });
      });
    })).then(() => { return result; });
  }

  static initializeSubsystemOptions(config, defaults) {
    // if the incoming config already has values (e.g., retrieved from Redis) then assume it already initialized
    // and do not apply defaults.
    // TODO: consider a mechanism for forcing flushing config. Would need to ensure this sync'd up well with whatever
    // the persistent config provider is doing.
    // TODO: here we are relying on an implementation detail of the config object that injects a `_config` prop. We
    // should update the config code to make that prop non enumerable so we don't have to filter it out everywhere.
    if (Object.getOwnPropertyNames(config).length > 1) {
      return Q(config);
    }
    return Q.all(Object.getOwnPropertyNames(defaults).map(optionName => {
      return config._config.set(optionName, defaults[optionName]);
    })).then(() => { return config._config.getAll(); });
  }

  static createRedisRefreshingConfig(crawlerName, subsystemName) {
    factoryLogger.info('Create refreshing redis config', { crawlerName: crawlerName, subsystemName: subsystemName });
    const redisClient = CrawlerFactory.getRedisClient(CrawlerFactory.createLogger(true));
    const key = `${crawlerName}:options:${subsystemName}`;
    const channel = `${key}-channel`;
    const configStore = new RefreshingConfigRedis.RedisConfigStore(redisClient, key);
    const config = new RefreshingConfig.RefreshingConfig(configStore)
      .withExtension(new RefreshingConfigRedis.RedisPubSubRefreshPolicyAndChangePublisher(redisClient, channel));
    return config;
  }

  static createInMemoryRefreshingConfig(values = {}) {
    factoryLogger.info('create in memory refreshing config');
    const configStore = new RefreshingConfig.InMemoryConfigStore(values);
    const config = new RefreshingConfig.RefreshingConfig(configStore)
      .withExtension(new RefreshingConfig.InMemoryPubSubRefreshPolicyAndChangePublisher());
    return config;
  }

  static createGitHubFetcher(store, options) {
    factoryLogger.info('create github fetcher');
    const requestor = CrawlerFactory.createRequestor();
    const tokenFactory = CrawlerFactory.createTokenFactory(options);
    const limiter = CrawlerFactory.createComputeLimiter(options);
    return new GitHubFetcher(requestor, store, tokenFactory, limiter, options);
  }

  static createTokenFactory(options) {
    factoryLogger.info('create token factory');
    const factory = new TokenFactory(config.get('CRAWLER_GITHUB_TOKENS'), options);
    const limiter = CrawlerFactory.createTokenLimiter(options);
    return new LimitedTokenFactory(factory, limiter, options);
  }

  static createRequestor() {
    factoryLogger.info('create requestor');
    return requestor.defaults({
      // turn off the requestor's throttle management mechanism in favor of ours
      forbiddenDelay: 0,
      delayOnThrottle: false
    });
  }

  static createFetcherMetrics(crawlerName, options) {
    factoryLogger.info('create fetcher metrics', { metricsStore: options.metricsStore });
    if (options.metricsStore !== 'redis') {
      return null;
    }
    const metrics = new RedisMetrics({ client: CrawlerFactory.getRedisClient(options.logger) });
    const names = ['fetch'];
    const result = {};
    names.forEach(name => {
      const fullName = `${crawlerName}:github:${name}`;
      result[name] = metrics.counter(fullName, { timeGranularity: 'second', namespace: 'crawlermetrics' }); // Stored in Redis as {namespace}:{name}:{period}
    });
    return result;
  }

  static createTokenLimiter(options) {
    factoryLogger.info('create token limiter', { capStore: options.capStore });
    return options.capStore === 'redis'
      ? CrawlerFactory.createRedisTokenLimiter(CrawlerFactory.getRedisClient(options.logger), options)
      : CrawlerFactory.createInMemoryTokenLimiter(options);
  }

  static createRedisTokenLimiter(redisClient, options) {
    factoryLogger.info('create redis token limiter', { callCapWindow: options.callCapWindow, callCapLimit: options.callCapLimit });
    const ip = '';
    return RedisRateLimiter.create({
      redis: redisClient,
      key: request => `${ip}:token:${request.key}`,
      window: () => options.callCapWindow || 1,
      limit: () => options.callCapLimit
    });
  }

  static createInMemoryTokenLimiter(options) {
    factoryLogger.info('create in memory token limiter', { callCapWindow: options.callCapWindow, callCapLimit: options.callCapLimit });
    return InMemoryRateLimiter.create({
      key: request => 'token:' + request.key,
      window: () => options.callCapWindow || 1,
      limit: () => options.callCapLimit
    });
  }

  static createComputeLimiter(options) {
    factoryLogger.info('create compute limiter', { computeLimitStore: options.computeLimitStore });
    const limiter = options.computeLimitStore === 'redis'
      ? CrawlerFactory.createRedisComputeLimiter(CrawlerFactory.getRedisClient(options.logger), options)
      : CrawlerFactory.createInMemoryComputeLimiter(options);
    options.baselineUpdater = CrawlerFactory._networkBaselineUpdater.bind(null, options.logger);
    return new ComputeLimiter(limiter, options);
  }

  static _networkBaselineUpdater(logger) {
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

  static createRedisComputeLimiter(redisClient, options) {
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

  static createInMemoryComputeLimiter(options) {
    factoryLogger.info('create in memory compute limiter', { computeWindow: options.computeWindow, computeLimit: options.computeLimit });
    return InMemoryRateLimiter.create({
      key: request => 'compute:' + request.key,
      incr: request => request.amount,
      window: () => options.computeWindow || 15,
      limit: () => options.computeLimit || 15000
    });
  }

  static createStore(options) {
    const provider = options.provider || 'azure';
    factoryLogger.info(`Create store for provider ${options.provider}`);
    let store = null;
    switch (options.provider) {
      case 'azure': {
        store = CrawlerFactory.createTableAndStorageStore(options);
        break;
      }
      case 'azure-redis': {
        store = CrawlerFactory.createRedisAndStorageStore(options);
        break;
      }
      case 'mongo': {
        store = CrawlerFactory.createMongoStore(options);
        break;
      }
      case 'memory': {
        store = new InMemoryDocStore(true);
        break;
      }
      case 'gcloudstorage': {
        store = CrawlerFactory.createGoogleCloudStore(options, config.get('CRAWLER_STORAGE_NAME'));
        break;
      }
      default: throw new Error(`Invalid store provider: ${provider}`);
    }
    store = CrawlerFactory.createDeltaStore(store, options);
    return store;
  }

  static createMongoStore(options) {
    return new MongoDocStore(config.get('CRAWLER_MONGO_URL'), options);
  }

  static createRedisAndStorageStore(options, name = null) {
    factoryLogger.info(`creating azure redis store`, { name: name });
    const baseStore = CrawlerFactory.createAzureStorageStore(options, name);
    return new UrlToUrnMappingStore(baseStore, CrawlerFactory.getRedisClient(options.logger), baseStore.name, options);
  }

  static createTableAndStorageStore(options, name = null) {
    factoryLogger.info(`creating azure store`, { name: name });
    const baseStore = CrawlerFactory.createAzureStorageStore(options, name);
    const account = config.get('CRAWLER_STORAGE_ACCOUNT');
    const key = config.get('CRAWLER_STORAGE_KEY');
    return new AzureTableMappingStore(baseStore, CrawlerFactory.createTableService(account, key), baseStore.name, options);
  }

  static createAzureStorageStore(options, name = null) {
    factoryLogger.info(`creating azure storage store`);
    name = name || config.get('CRAWLER_STORAGE_NAME');
    const account = config.get('CRAWLER_STORAGE_ACCOUNT');
    const key = config.get('CRAWLER_STORAGE_KEY');
    const blobService = CrawlerFactory.createBlobService(account, key);
    return new AzureStorageDocStore(blobService, name, options);
  }

  static createGoogleCloudStore(options, name = null) {
    factoryLogger.info(`creating Google Cloud Storage store`);
    const projectId = config.get('CRAWLER_GOOGLE_STORAGE_PROJECT_ID');
    const clientEmail = config.get('CRAWLER_GOOGLE_STORAGE_CLIENT_EMAIL');
    const key = config.get('CRAWLER_GOOGLE_STORAGE_KEY');
    if (!projectId) {
      factoryLogger.error(`you must provide a 'CRAWLER_GOOGLE_STORAGE_PROJECT_ID' value to use Google Cloud Storage.`);
    }
    if (!clientEmail) {
      factoryLogger.error(`you must provide a 'CRAWLER_GOOGLE_STORAGE_CLIENT_EMAIL' value to use Google Cloud Storage.`);
    }
    if (!key) {
      factoryLogger.error(`you must provide a 'CRAWLER_GOOGLE_STORAGE_KEY' value to use Google Cloud Storage.`);
    }
    return new GoogleCloudStorage(name, projectId, clientEmail, key, options);
  }

  static createDeadletterStore(options) {
    const provider = options.provider || 'azure';
    factoryLogger.info(`Create deadletter store for provider ${options.provider}`);
    switch (options.provider) {
      case 'azure':
      case 'azure-redis': {
        return CrawlerFactory.createAzureStorageStore(options, config.get('CRAWLER_STORAGE_NAME') + '-deadletter');
      }
      case 'mongo': {
        return CrawlerFactory.createMongoStore(options);
      }
      case 'memory': {
        return new InMemoryDocStore(true);
      }
      case 'gcloudstorage': {
        return CrawlerFactory.createGoogleCloudStore(options, config.get('CRAWLER_STORAGE_NAME') + '-deadletter');
      }
      default: throw new Error(`Invalid store provider: ${provider}`);
    }
  }

  static createDeltaStore(inner, options) {
    if (!options.delta || !options.delta.provider || options.delta.provider === 'none') {
      return inner;
    }
    factoryLogger.info(`creating delta store`);
    const deltaStoreProviders = typeof options.delta.provider === 'string' ? [options.delta.provider] : options.delta.provider;
    let store = inner;
    deltaStoreProviders.forEach(deltaProvider => {
      switch (deltaProvider) {
        case 'azure':
        case 'azure-redis':
          store = CrawlerFactory.createAzureDeltaStore(store, null, options);
          break;
        case 'gcloudstorage':
          store = CrawlerFactory.createGcloudDeltaStore(store, null, options);
          break;
        default:
          try {
            const PluggableDeltaStore = require(`ghcrawler-${deltaProvider}`);
            store = new PluggableDeltaStore(store, options);
          } catch (error) {
            factoryLogger.error(error);
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
    factoryLogger.info('creating delta store', { name: name, account: account });
    const blobService = CrawlerFactory.createBlobService(account, key);
    return new DeltaStore(inner, blobService, name, options);
  }

  static createGcloudDeltaStore(inner, name = null, options = {}) {
    name = name || config.get('CRAWLER_DELTA_STORAGE_NAME') || `${config.get('CRAWLER_STORAGE_NAME')}-log`;
    const projectId = config.get('CRAWLER_DELTA_GOOGLE_STORAGE_PROJECT_ID') || config.get('CRAWLER_GOOGLE_STORAGE_PROJECT_ID');
    const clientEmail = config.get('CRAWLER_DELTA_GOOGLE_STORAGE_CLIENT_EMAIL') || config.get('CRAWLER_GOOGLE_STORAGE_CLIENT_EMAIL');
    const key = config.get('CRAWLER_DETLA_GOOGLE_STORAGE_KEY') || config.get('CRAWLER_GOOGLE_STORAGE_KEY');
    factoryLogger.info('creating google cloud delta store', { name });

    // Environment variables will cause new lines to be encoded as'\\n'
    // which causes the google-cloud storage SDK to fail as it requires
    // '\n' characters.
    const parsedPrivateKey = key.replace(/\\n/g, '\n');

    const deltaStorage = new GoogleStorage({
      projectId,
      credentials: {
        client_email: clientEmail,
        private_key: parsedPrivateKey,
      }
    });

    return new GoogleCloudDeltaStorage(inner, deltaStorage, name, options);
  }

  static getRedisClient(logger) {
    factoryLogger.info('retrieving redis client');
    if (redisClient) {
      return redisClient;
    }
    const url = config.get('CRAWLER_REDIS_URL');
    const port = config.get('CRAWLER_REDIS_PORT');
    const key = config.get('CRAWLER_REDIS_ACCESS_KEY');
    const tls = config.get('CRAWLER_REDIS_TLS') === 'true';
    redisClient = CrawlerFactory.createRedisClient(url, key, port, tls, logger);
    return redisClient;
  }

  static createRedisClient(url, key, port, tls, logger) {
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

  static createBlobService(account, key) {
    factoryLogger.info(`creating blob service`);
    const retryOperations = new AzureStorage.ExponentialRetryPolicyFilter();
    return AzureStorage.createBlobService(account, key).withFilter(retryOperations);
  }

  static createTableService(account, key) {
    factoryLogger.info(`creating table service`);
    const retryOperations = new AzureStorage.ExponentialRetryPolicyFilter();
    return AzureStorage.createTableService(account, key).withFilter(retryOperations);
  }

  static createLocker(options) {
    factoryLogger.info(`creating locker`, { provider: options.provider });
    if (options.provider === 'memory') {
      return CrawlerFactory.createNolock();
    }
    return new redlock([CrawlerFactory.getRedisClient(options.logger)], {
      driftFactor: 0.01,
      retryCount: options.retryCount,
      retryDelay: options.retryDelay
    });
  }

  static createLogger(echo = false, level = 'info') {
    mockInsights.setup(config.get('CRAWLER_INSIGHTS_KEY') || 'mock', echo);
    const result = new winston.Logger();
    result.add(aiLogger, {
      insights: appInsights,
      treatErrorsAsExceptions: true,
      exitOnError: false,
      level: level
    });
    return result;
  }

  static createRequestTracker(prefix, options) {
    let locker = null;
    if (options.tracker.locking) {
      locker = new redlock([CrawlerFactory.getRedisClient(options.logger)], options.tracker);
    } else {
      locker = CrawlerFactory.createNolock();
    }
    return new RedisRequestTracker(prefix, CrawlerFactory.getRedisClient(options.logger), locker, options);
  }

  static createNolock() {
    return { lock: () => null, unlock: () => { } };
  }

  static createQueues(options) {
    const provider = options.provider || 'amqp10';
    if (provider === 'amqp10') {
      return CrawlerFactory.createAmqp10Queues(options);
    } else if (provider === 'amqp') {
      return CrawlerFactory.createAmqpQueues(options);
    } else if (provider === 'memory') {
      return CrawlerFactory.createMemoryQueues(options);
    } else {
      throw new Error(`Invalid queue provider option: ${provider}`);
    }
  }

  static createAmqpQueues(options) {
    const managementEndpoint = config.get('CRAWLER_RABBIT_MANAGER_ENDPOINT');
    const url = config.get('CRAWLER_AMQP_URL');
    const manager = new RabbitQueueManager(url, managementEndpoint, options.socketOptions ? options.socketOptions.ca : null);
    const env = process.env.NODE_ENV;
    const tracker = CrawlerFactory.createRequestTracker(`${env}:AMQP:${options.queueName}`, options);
    return CrawlerFactory.createQueueSet(manager, tracker, options);
  }

  static createAmqp10Queues(options) {
    const managementEndpoint = config.get('CRAWLER_SERVICEBUS_MANAGER_ENDPOINT');
    const amqpUrl = config.get('CRAWLER_AMQP10_URL');
    const manager = new ServiceBusQueueManager(amqpUrl, managementEndpoint);
    const env = process.env.NODE_ENV;
    const tracker = CrawlerFactory.createRequestTracker(`${env}:AMQP10:${options.queueName}`, options);
    return CrawlerFactory.createQueueSet(manager, tracker, options);
  }

  static createMemoryQueues(options) {
    const manager = {
      createQueueChain: (name, tracker, options) => {
        return CrawlerFactory.createMemoryQueue(name, options);
      }
    };
    return CrawlerFactory.createQueueSet(manager, null, options);
  }

  static createQueueSet(manager, tracker, options) {
    const immediate = manager.createQueueChain('immediate', tracker, options);
    const soon = manager.createQueueChain('soon', tracker, options);
    const normal = manager.createQueueChain('normal', tracker, options);
    const later = manager.createQueueChain('later', tracker, options);
    const queues = CrawlerFactory.addEventQueue(manager, [immediate, soon, normal, later], options);
    return new QueueSet(queues, options);
  }

  static createMemoryQueue(name, options) {
    return new AttenuatedQueue(new InMemoryCrawlQueue(name, options), options);
  }

  static addEventQueue(manager, queues, options) {
    if (options.events.provider && options.events.provider !== 'none') {
      queues.unshift(CrawlerFactory.createEventQueue(manager, options));
    }
    return queues;
  }

  static createEventQueue(manager, options) {
    if (options.events.provider === 'amqp10') {
      return CrawlerFactory.createAmqp10EventSubscription(options);
    }
    if (options.events.provider === 'webhook') {
      return manager.createQueueChain('events', null, options);
    }
    throw new Error(`No event provider for ${options.events.provider}`);
  }

  static createAmqp10EventSubscription(options) {
    const amqpUrl = config.get('CRAWLER_EVENT_AMQP10_URL');
    const actualClient = new AmqpClient(AmqpPolicy.ServiceBusQueue);
    const client = actualClient.connect(amqpUrl).then(() => { return actualClient; });
    const formatter = new EventFormatter(options);
    const queueName = `${options.events.topic}/Subscriptions/${options.events.queueName}`;
    const result = new Amqp10Queue(client, 'events', queueName, formatter.format.bind(formatter), null, options);
    result.mode = { receive: 'receive' };
    return result;
  }

  static createQueuingMetrics(crawlerName, options) {
    if (options.metricsStore !== 'redis') {
      return null;
    }
    const metrics = new RedisMetrics({ client: CrawlerFactory.getRedisClient(options.logger) });
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

  static loadOrgs() {
    let orgList = config.get('CRAWLER_ORGS');
    if (orgList) {
      orgList = orgList.split(';').map(entry => entry.toLowerCase().trim());
    } else {
      orgList = CrawlerFactory._loadLines(config.get('CRAWLER_ORGS_FILE'));
    }
    return orgList;
  }

  static _loadLines(path) {
    if (!path || !fs.existsSync(path)) {
      return [];
    }
    let result = fs.readFileSync(path, 'utf8');
    result = result.split(/\s/);
    return result.filter(line => { return line; }).map(line => { return line.toLowerCase(); });
  }
}

factoryLogger = CrawlerFactory.createLogger(true);

module.exports = CrawlerFactory;

class EventFormatter {
  constructor(options) {
    this.options = options;
    this.logger = options.logger;
  }

  format(message) {
    // The message here is expected to be a WEBHOOK event.  Use the information included to identify the
    // repo or org to poll for new events.
    const type = message.applicationProperties.event;
    const event = message.body;
    const eventsUrl = event.repository ? event.repository.events_url : event.organization.events_url;
    const result = new Request('event_trigger', `${eventsUrl}`);
    result.payload = { body: event, etag: 1, fetchedAt: moment.utc().toISOString() };
    // requests off directly off the event feed do not need exclusivity
    request.requiresLock = false;
    // if the event is for a private repo, mark the request as needing private access.
    if (event.repository && event.repository.private) {
      request.context.repoType = 'private';
    }
    // mark it to be retried on the immediate queue as we don't want to requeue it on this shared topic
    request._retryQueue = 'immediate';
    return request;
  }
}

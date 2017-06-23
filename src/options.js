const config = require('painless-config');
const fs = require('fs');
const createLogger = require('./logging').createLogger;
const logger = createLogger(true);
const RefreshingConfig = require('refreshing-config');
const RefreshingConfigRedis = require('refreshing-config-redis');

const redisUtil = require('./factory/util/redis');
const deepAssign = require('deep-assign');

function loadLines(path) {
  if (!path || !fs.existsSync(path)) {
    return [];
  }
  let result = fs.readFileSync(path, 'utf8');
  result = result.split(/\s/);
  return result.filter(line => { return line; }).map(line => { return line.toLowerCase(); });
}

function loadOrgs() {
  let orgList = config.get('CRAWLER_ORGS');
  if (orgList) {
    orgList = orgList.split(';').map(entry => entry.toLowerCase().trim());
  } else {
    orgList = loadLines(config.get('CRAWLER_ORGS_FILE'));
  }
  return orgList;
}

function loadOptions(options) {
  options = options || {};

  let defaultOptions = {
    crawler: {
      name: config.get('CRAWLER_NAME') || 'crawler',
      count: 0,
      pollingDelay: 5000,
      processingTtl: 60 * 1000,
      promiseTrace: false,
      requeueDelay: 5000,
      orgList: options.orgs || loadOrgs()
    },
    fetcher: {
      githubTokens: options.githubTokens || config.get('CRAWLER_GITHUB_TOKENS'),
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

  var finalOptions = deepAssign(defaultOptions, options);
  console.log('finalOptions:', finalOptions);
  return finalOptions;
}

function createRedisRefreshingConfig(crawlerName, subsystemName) {
  logger.info('Create refreshing redis config', { crawlerName: crawlerName, subsystemName: subsystemName });
  const redisClient = redisUtil.getRedisClient(createLogger(true));
  const key = `${crawlerName}:options:${subsystemName}`;
  const channel = `${key}-channel`;
  const configStore = new RefreshingConfigRedis.RedisConfigStore(redisClient, key);
  const config = new RefreshingConfig.RefreshingConfig(configStore)
    .withExtension(new RefreshingConfigRedis.RedisPubSubRefreshPolicyAndChangePublisher(redisClient, channel));
  return config;
}

function createInMemoryRefreshingConfig(values = {}) {
  logger.info('create in memory refreshing config');
  const configStore = new RefreshingConfig.InMemoryConfigStore(values);
  const config = new RefreshingConfig.RefreshingConfig(configStore)
    .withExtension(new RefreshingConfig.InMemoryPubSubRefreshPolicyAndChangePublisher());
  return config;
}

function initializeSubsystemOptions(config, defaults) {

  if (Object.getOwnPropertyNames(config).length > 1) {
    return Promise.resolve(config);
  }
  return Promise.all(Object.getOwnPropertyNames(defaults).map(optionName => {
      return config._config.set(optionName, defaults[optionName]);
    }))
    .then(() => { return config._config.getAll(); });
}

function createRefreshingOptions(crawlerName, subsystemNames, provider = 'redis', options = {}) {
  logger.info(`creating refreshing options with crawlerName:${crawlerName}`);

  provider = provider.toLowerCase();
  options = loadOptions(options);

  return Promise.all(subsystemNames.map(subsystemName => {
      logger.info(`creating refreshing options promise with crawlerName:${crawlerName} subsystemName ${subsystemName} provider ${provider}`);
      let config = null;
      if (provider === 'redis') {
        config = createRedisRefreshingConfig(crawlerName, subsystemName);
      } else if (provider === 'memory') {
        config = createInMemoryRefreshingConfig();
      } else {
        throw new Error(`Invalid options provider setting ${provider}`);
      }
      return config.getAll().then(values => {
        logger.info(`creating refreshingOption config get completed`);


        return initializeSubsystemOptions(values, options[subsystemName]).then(resolved => {
          logger.info(`subsystem options initialized`);

          options[subsystemName] = values;
        });
      });
    }))
    .then(() => {
      return options;
    });
}

exports.createRefreshingOptions = createRefreshingOptions;

const factoryLogger = require('./util/logger');
const InMemoryDocStore = require('../providers/storage/InMemoryDocStore');
const MongoDocStore = require('../providers/storage/MongoDocStore');
const AzureTableMappingStore = require('../providers/storage/AzureTableMappingStore');
const UrlToUrnMappingStore = require('../providers/storage/UrlToUrnMappingStore');
const AzureStorageDocStore = require('../providers/storage/AzureStorageDocStore');
const AzureDeltaStore = require('../providers/storage/AzureDeltaStore');
const azureUtil = require('./util/azure');
const redisUtil = require('./util/redis');
const config = require('painless-config');

function createAzureStorageStore(options, name = null) {
  factoryLogger.info(`creating azure storage store`);
  name = name || config.get('CRAWLER_STORAGE_NAME');
  const account = config.get('CRAWLER_STORAGE_ACCOUNT');
  const key = config.get('CRAWLER_STORAGE_KEY');
  const blobService = azureUtil.createBlobService(account, key);
  return new AzureStorageDocStore(blobService, name, options);
}

function createTableAndStorageStore(options, name = null) {
  factoryLogger.info(`creating azure store`, { name: name });
  const baseStore = createAzureStorageStore(options, name);
  const account = config.get('CRAWLER_STORAGE_ACCOUNT');
  const key = config.get('CRAWLER_STORAGE_KEY');
  return new AzureTableMappingStore(baseStore, azureUtil.createTableService(account, key), baseStore.name, options);
}

function createRedisAndStorageStore(options, name = null) {
  factoryLogger.info(`creating azure redis store`, { name: name });
  const baseStore = createAzureStorageStore(options, name);
  return new UrlToUrnMappingStore(baseStore, redisUtil.getRedisClient(options.logger), baseStore.name, options);
}

function createMongoStore(options) {
  return new MongoDocStore(config.get('CRAWLER_MONGO_URL'), options);
}

function createAzureDeltaStore(inner, name = null, options = {}) {
  name = name || config.get('CRAWLER_DELTA_STORAGE_NAME') || `${config.get('CRAWLER_STORAGE_NAME')}-log`;
  const account = config.get('CRAWLER_DELTA_STORAGE_ACCOUNT') || config.get('CRAWLER_STORAGE_ACCOUNT');
  const key = config.get('CRAWLER_DELTA_STORAGE_KEY') || config.get('CRAWLER_STORAGE_KEY');
  factoryLogger.info('creating delta store', { name: name, account: account });
  const blobService = azureUtil.createBlobService(account, key);
  return new AzureDeltaStore(inner, blobService, name, options);
}

function createDeltaStore(inner, options) {
  if (!options.delta || !options.delta.provider || options.delta.provider === 'none') {
    return inner;
  }
  factoryLogger.info(`creating delta store`);
  switch (options.delta.provider) {
    case 'azure':
    case 'azure-redis': {
      return createAzureDeltaStore(inner, null, options);
    }
    default: throw new Error(`Invalid delta store provider: ${options.delta.provider}`);
  }
}

function createStore(options) {
  const provider = options.provider || 'azure';
  factoryLogger.info(`Create store for provider ${options.provider}`);
  let store = null;
  switch (options.provider) {
    case 'azure': {
      store = createTableAndStorageStore(options);
      break;
    }
    case 'azure-redis': {
      store = createRedisAndStorageStore(options);
      break;
    }
    case 'mongo': {
      store = createMongoStore(options);
      break;
    }
    case 'memory': {
      store = new InMemoryDocStore(true);
      break;
    }
    default: throw new Error(`Invalid store provider: ${provider}`);
  }
  store = createDeltaStore(store, options);
  return store;
}

function createDeadletterStore(options) {
  const provider = options.provider || 'azure';
  factoryLogger.info(`Create deadletter store for provider ${options.provider}`);
  switch (options.provider) {
    case 'azure':
    case 'azure-redis': {
      return createAzureStorageStore(options, config.get('CRAWLER_STORAGE_NAME') + '-deadletter');
    }
    case 'mongo': {
      return createMongoStore(options);
    }
    case 'memory': {
      return new InMemoryDocStore(true);
    }
    default: throw new Error(`Invalid store provider: ${provider}`);
  }
}

exports.createStore = createStore;
exports.createDeadletterStore = createDeadletterStore;
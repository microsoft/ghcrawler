// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Crawler = require('../Crawler');
const createLogger = require('../logging').createLogger;
const factoryLogger = require('./util/logger');
const fetcherFactory = require('./fetcherFactory');
const GitHubProcessor = require('../providers/fetcher/GitHubProcessor');
const lockingFactory = require('./lockingFactory');
const metricsFactory = require('./metricsFactory');
const Q = require('q');
const queuingFactory = require('./queuingFactory');
const storageFactory = require('./storageFactory');

function decorateOptions(options) {
  Object.getOwnPropertyNames(options).forEach(key => {
    const logger = createLogger(true);
    options[key].logger = logger;
    const capitalized = key.charAt(0).toUpperCase() + key.slice(1);
    const metricsFactoryFunc = metricsFactory[`create${capitalized}Metrics`];
    if (metricsFactoryFunc) {
      factoryLogger.info('Creating metrics factory', { factory: capitalized });
      logger.metrics = metricsFactoryFunc(options.crawler.name, options[key]);
    }
  });
}

function _configureInMemoryOptions(options) {
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

function createCrawler(options, { queues = null, store = null, deadletters = null, locker = null, fetcher = null, processor = null } = {}) {
  decorateOptions(options);
  queues = queues || queuingFactory.createQueues(options.queuing);
  store = store || storageFactory.createStore(options.storage);
  deadletters = deadletters || storageFactory.createDeadletterStore(options.storage);
  locker = locker || lockingFactory.createLocker(options.locker);
  fetcher = fetcher || fetcherFactory.createGitHubFetcher(store, options.fetcher);
  processor = processor || new GitHubProcessor(store);
  const crawler = new Crawler(queues, store, deadletters, locker, fetcher, processor, options.crawler);
  crawler.initialize = function () {
    return Q.try(crawler.queues.subscribe.bind(crawler.queues))
      .then(crawler.store.connect.bind(crawler.store))
      .then(crawler.deadletters.connect.bind(crawler.deadletters));
  }

  return crawler;
}

function createStandardCrawler(options) {
  factoryLogger.info(`creating standard Crawler Started`);
  return createCrawler(options);
}

function createInMemoryCrawler(options) {
  _configureInMemoryOptions(options);
  return createCrawler(options);
}

exports.createStandardCrawler = createStandardCrawler;
exports.createInMemoryCrawler = createInMemoryCrawler;
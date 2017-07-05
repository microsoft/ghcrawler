// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const crawlerDefaults = {
  name: config.get('CRAWLER_NAME') || 'crawler',
  count: 1
};

const fetcherDefaults = {
  githubTokens: options.githubTokens || config.get('CRAWLER_GITHUB_TOKENS'),
  callCapStore: 'memory',
  computeLimitStore: 'memory'
};

const queuingDefaults = {
  weights: { events: 10, immediate: 3, soon: 2, normal: 3, later: 2 },
};

module.exports = function loadConfig() {
  const services = {
    logger: createLogger(),
    configFactory: createRefreshingConfigFactory()
  };

  // Instantiate each of the required services and add them to the service list.
  // Each service module has one or more factory functions that instantiates
  // the service and addes it to the supplied list of services with the (optionally)
  // given name.  The service may use or connect to other services supplied in the
  // given service list. The factory function also returns the newly created service.
  // Factory functions
  require('redisLocker').create('locker', services);
  require('mongoDocStore').create('store', services);
  require('amqpQueues').create('queuing', services, queuingDefaults);
  require('mongoDocStore').create('deadletters', services);
  require('githubFetcher').create('fetcher', services, fetcherDefaults);
  require('githubProcessor').create('processor', services);
  require('crawler').create('crawler', services, crawlerDefaults);
  return services;
}


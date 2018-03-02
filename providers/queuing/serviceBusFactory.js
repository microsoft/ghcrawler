// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const ServiceBusQueueManager = require('./serviceBusQueueManager');
const CrawlerFactory = require('../../crawlerFactory');

// {
//   connectionString: config.get('CRAWLER_SERVICEBUS_CONNECTION_STRING') || config.get('CRAWLER_SERVICEBUS_MANAGER_ENDPOINT')
// }

module.exports = options => {
  const { connectionString } = options;
  const manager = new ServiceBusQueueManager(null, connectionString);
  const env = process.env.NODE_ENV;
  let tracker;
  if (options.tracker) {
    tracker = CrawlerFactory.createRequestTracker(`${env}:ServiceBus:${options.queueName}`, options);
  }
  return CrawlerFactory.createQueueSet(manager, tracker, options);
}
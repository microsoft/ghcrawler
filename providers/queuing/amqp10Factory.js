// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const ServiceBusQueueManager = require('./serviceBusQueueManager');
const CrawlerFactory = require('../../CrawlerFactory');

// {
//   managementEndpoint: config.get('CRAWLER_SERVICEBUS_MANAGER_ENDPOINT'),
//   url: config.get('CRAWLER_AMQP10_URL')
// }

module.exports = options => {
  const { managementEndpoint, url } = options;
  const manager = new ServiceBusQueueManager(url, managementEndpoint);
  const env = process.env.NODE_ENV;
  const tracker = CrawlerFactory.createRequestTracker(`${env}:AMQP10:${options.queueName}`, options);
  return CrawlerFactory.createQueueSet(manager, tracker, options);
}

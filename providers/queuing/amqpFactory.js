// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const RabbitQueueManager = require('./rabbitQueueManager');
const CrawlerFactory = require('../../crawlerFactory');

// {
//   managementEndpoint: config.get('CRAWLER_RABBIT_MANAGER_ENDPOINT'),
//   url: config.get('CRAWLER_AMQP_URL')
// }

module.exports = options => {
  const { managementEndpoint, url } = options;
  const manager = new RabbitQueueManager(url, managementEndpoint, options.socketOptions ? options.socketOptions.ca : null);
  const env = process.env.NODE_ENV;
  const tracker = CrawlerFactory.createRequestTracker(`${env}:AMQP:${options.queueName}`, options);
  return CrawlerFactory.createQueueSet(manager, tracker, options);
}

// Copyright (c) Microsoft Corporation and others. Made available under the MIT license.
// SPDX-License-Identifier: MIT

const StorageQueueManager = require('./storageQueueManager');
const CrawlerFactory = require('../../crawlerFactory');

module.exports = options => {
  const { connectionString } = options;
  const manager = new StorageQueueManager(connectionString, options);
  const env = process.env.NODE_ENV;
  let tracker;
  if (options.tracker) {
    tracker = CrawlerFactory.createRequestTracker(`${env}:ServiceBus:${options.queueName}`, options);
  }
  return CrawlerFactory.createQueueSet(manager, tracker, options);
}

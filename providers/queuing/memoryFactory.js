// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const CrawlerFactory = require('../../CrawlerFactory');
const AttenuatedQueue = require('./attenuatedQueue');
const InMemoryCrawlQueue = require('./inmemorycrawlqueue');

module.exports = options => {
  const manager = {
    createQueueChain: (name, tracker, options) => {
      return new AttenuatedQueue(new InMemoryCrawlQueue(name, options), options);
    }
  };
  return CrawlerFactory.createQueueSet(manager, null, options);
}
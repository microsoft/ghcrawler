// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const CrawlerFactory = require('../../crawlerFactory');
const redlock = require('redlock');

module.exports = options => {
  return new redlock([CrawlerFactory.getProvider('redis')], {
    driftFactor: 0.01,
    retryCount: options.retryCount,
    retryDelay: options.retryDelay
  });
}
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const server = require('./server');
const factory = require('./factory');

exports.start = function(options) {
  const service = factory.createService(options);
  server.start(service);
}

// TBD Are the things below needed?
exports.crawler = require('./crawler');
exports.crawlerService = require('./crawlerService');
exports.factory = require('./factory');
exports.githubFetcher = require('../providers/fetcher/githubFetcher');
exports.githubProcessor = require('../providers/fetcher/githubProcessor');
exports.policy = require('./traversalPolicy');
exports.queueSet = require('../providers/queuing/queueSet');
exports.request = require('./request');
exports.traversalPolicy = require('./traversalPolicy');
// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

module.exports.crawler = require('./lib/crawler');
module.exports.crawlerService = require('./lib/crawlerService');
module.exports.crawlerFactory = require('./crawlerFactory');
module.exports.policy = require('./lib/traversalPolicy');
module.exports.queueSet = require('./providers/queuing/queueSet');
module.exports.request = require('./lib/request');
module.exports.traversalPolicy = require('./lib/traversalPolicy');
module.exports.visitorMap = require('./lib/visitorMap');

const www = require('./bin/www');
const crawlerFactory = require('./crawlerFactory');

module.exports.run = (defaults, searchPath) => {
  const service = crawlerFactory.createService(defaults, searchPath);
  www(service);
}
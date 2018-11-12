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
module.exports.providers = require('./providers')

const www = require('./bin/www');
const CrawlerFactory = require('./crawlerFactory');
const VisitorMap = require('./lib/visitorMap');

module.exports.run = (defaults, logger, searchPath, maps) => {
  const service = CrawlerFactory.createService(defaults, logger, searchPath);
  Object.getOwnPropertyNames(maps).forEach(name =>
    VisitorMap.register(name, maps[name]))
  www(service, logger);
}

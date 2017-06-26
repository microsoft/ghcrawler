#!/usr/bin/env node
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
const start = require('../src').start;
const path = require('path');

const args = require('argly').createParser({
    '--config -c *': 'string'
  })
  .parse();

Promise.resolve()
  .then(function loadOptions() {
    var configFile = args.config || process.env.CRAWLER_CONFIG;
    // Load options
    if (configFile) {
      var configFunc = require(path.resolve(process.cwd(), configFile));
      return configFunc();
    } else {
      return {};
    }
  })
  .then(function(options) {
    // Start the server
    start(options);
  })
  .catch(function(err) {
    console.error('An error has occurred: ' + err, err);
    process.exit(1);
  });


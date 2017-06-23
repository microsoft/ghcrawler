// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const server = require('./server');
const factory = require('./factory');

exports.start = function(options) {
  const service = factory.createService(options);
  server.start(service);
}
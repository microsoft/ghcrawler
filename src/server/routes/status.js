// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const auth = require('../middleware/auth');
const express = require('express');

let crawlerService = null;
const router = express.Router();

router.get('/', auth.validate, function (request, response, next) {
  // Gets some of the live, non-configurable values and put them in at the root
  const result = {};
  result.actualCount = crawlerService.status();
  const loop = crawlerService.loops[0];
  if (loop) {
    result.delay = loop.options.delay || 0;
  }

  response.status(200).send(result);
});

function setup(service) {
  crawlerService = service;
  return router;
}

module.exports = setup;
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const auth = require('../middleware/auth');
const express = require('express');
const wrap = require('../middleware/promiseWrap');

let crawlerService = null;
const router = express.Router();

router.put('/:name', auth.validate, wrap(function* (request, response) {
  const result = yield crawlerService.flushQueue(request.params.name);
  if (!result) {
    return response.sendStatus(404);
  }
  response.sendStatus(200);
}));

router.get('/:name/info', auth.validate, wrap(function* (request, response) {
  const info = yield crawlerService.getQueueInfo(request.params.name);
  if (!info) {
    return response.sendStatus(404);
  }
  response.json(info);
}));

function setup(service) {
  crawlerService = service;
  return router;
}
module.exports = setup;
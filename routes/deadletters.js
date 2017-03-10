// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const auth = require('../middleware/auth');
const express = require('express');
const expressJoi = require('express-joi');
const Request = require('ghcrawler').request;
const wrap = require('../middleware/promiseWrap');

let crawlerService = null;
const router = express.Router();

router.head('/', auth.validate, wrap(function* (request, response) {
  const count = yield crawlerService.getDeadletterCount();
  response.setHeader('X-Total-Count', count);
  response.status(204).end();
}));

router.get('/', auth.validate, wrap(function* (request, response) {
  const requests = yield crawlerService.listDeadletters();
  response.setHeader('X-Total-Count', requests.length);
  response.json(requests);
}));

router.get('/:urn', auth.validate, wrap(function* (request, response) {
  const document = yield crawlerService.getDeadletter(request.params.urn);
  response.json(document);
}));

router.delete('/:urn', auth.validate, wrap(function* (request, response) {
  let requeue = request.query.requeue;
  if (requeue) {
    yield crawlerService.requeueDeadletter(request.params.urn, requeue);
  } else {
    yield crawlerService.deleteDeadletter(request.params.urn);
  }
  response.status(204).end();
}));

function setup(service) {
  crawlerService = service;
  return router;
}
module.exports = setup;
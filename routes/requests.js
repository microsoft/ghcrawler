// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const auth = require('../middleware/auth');
const express = require('express');
const expressJoi = require('express-joi');
const Request = require('ghcrawler').request;
const TraversalPolicy = require('ghcrawler').traversalPolicy;
const wrap = require('../middleware/promiseWrap');

const requestsSchema = {
  queue: expressJoi.Joi.types.String().alphanum().min(2).max(50).required(),
  count: expressJoi.Joi.types.Number().integer().min(0).max(100)
};
const queueSchema = {
  name: expressJoi.Joi.types.String().alphanum().min(2).max(50).required()
};

let crawlerService = null;
const router = express.Router();

router.post('/:queue?', auth.validate, wrap(function* (request, response) {
  const result = yield queueRequests(request.body, request.params.queue || 'normal');
  if (!result) {
    return response.sendStatus(404);
  }
  response.sendStatus(201);
}));

router.get('/:queue', auth.validate, expressJoi.joiValidate(requestsSchema), wrap(function* (request, response) {
  const requests = yield crawlerService.getRequests(request.params.queue, parseInt(request.query.count, 10), false);
  if (!requests) {
    return response.sendStatus(404);
  }
  response.json(requests);
}));

router.delete('/:queue', auth.validate, expressJoi.joiValidate(requestsSchema), wrap(function* (request, response) {
  const requests = yield crawlerService.getRequests(request.params.queue, parseInt(request.query.count, 10), true);
  if (!requests) {
    return response.sendStatus(404);
  }
  response.json(requests);
}));

function queueRequests(requestSpecs, queueName) {
  requestSpecs = Array.isArray(requestSpecs) ? requestSpecs : [requestSpecs];
  const requests = requestSpecs.map(spec => rationalizeRequest(spec));
  return crawlerService.queue(requests, queueName).catch(error => {
    if (error.message && error.message.startsWith('Queue not found')) {
      return null;
    }
    throw error;
  });
}

function rationalizeRequest(request) {
  let result = request;
  if (typeof request === 'string') {
    request = buildRequestFromSpec(request);
  }
  return Request.adopt(request);
}

function buildRequestFromSpec(spec) {
  let crawlType = null;
  let crawlUrl = 'https://api.github.com/';
  if (spec.indexOf('/') > -1) {
    crawlType = 'repo';
    crawlUrl += 'repos/' + spec;
  } else {
    crawlType = 'org';
    crawlUrl += 'orgs/' + spec;
  }

  return {
    "type": crawlType,
    "url": crawlUrl,
    "policy": "default"
  };
}

function setup(service) {
  crawlerService = service;
  return router;
}
module.exports = setup;
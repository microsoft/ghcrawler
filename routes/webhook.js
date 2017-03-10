// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const crypto = require('crypto');
const express = require('express');
const moment = require('moment');
const Request = require('ghcrawler').request;
const wrap = require('../middleware/promiseWrap');

let crawlerService = null;
let webhookSecret = null;
const router = express.Router();

router.post('/', wrap(function* (request, response, next) {
  if (crawlerService.options.queuing.events.provider !== 'webhook') {
    return warn(request, response, 'Webhooks not enabled');
  }
  getLogger().verbose('Received', `Webhook event`, {delivery: request.headers['x-github-delivery']});
  const signature = request.headers['x-hub-signature'];
  const eventType = request.headers['x-github-event'];

  if (!signature || !eventType) {
    return fatal(request, response, 'Missing signature or event type on GitHub webhook');
  }

  const data = request.body;
  const computedSignature = 'sha1=' + crypto.createHmac('sha1', webhookSecret).update(data).digest('hex');
  if (!crypto.timingSafeEqual(new Buffer(signature), new Buffer(computedSignature))) {
    return fatal(request, response, 'X-Hub-Signature does not match blob signature');
  }
  const event = JSON.parse(request.body);
  const eventsUrl = event.repository ? event.repository.events_url : event.organization.events_url;
  const result = new Request('event_trigger', `${eventsUrl}`);
  result.payload = { body: event, etag: 1, fetchedAt: moment.utc().toISOString() };
  // requests off directly off the event feed do not need exclusivity
  result.requiresLock = false;
  // if the event is for a private repo, mark the request as needing private access.
  if (event.repository && event.repository.private) {
    result.context.repoType = 'private';
  }
  yield crawlerService.queue(result, 'events');
  getLogger().info('Queued', `Webhook event for ${eventsUrl}`, {delivery: request.headers['x-github-delivery']});

  response.status(200).end();
}));

function warn(request, response, message) {
  getLogger().warn(fatal, { delivery: request.headers['x-github-delivery']});
  response.status(500);
  response.setHeader('content-type', 'text/plain');
  response.end(JSON.stringify(fatal));
}

function fatal(request, response, error) {
  getLogger().error(error, { delivery: request.headers['x-github-delivery']});
  response.status(400);
  response.setHeader('content-type', 'text/plain');
  response.end(JSON.stringify(error));
}

function getLogger() {
  return crawlerService.crawler.logger;
}

function setup(service, secret) {
  crawlerService = service;
  webhookSecret = secret;
  return router;
}

module.exports = setup;

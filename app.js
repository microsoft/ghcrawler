// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const appInsights = require('applicationinsights');
const auth = require('./middleware/auth');
const bodyParser = require('body-parser');
const config = require('painless-config');
const CrawlerService = require('ghcrawler').crawlerService;
const express = require('express');
const logger = require('morgan');
const mockInsights = require('./providers/logger/mockInsights');
const CrawlerFactory = require('./lib/crawlerFactory');
const sendHelper = require('./middleware/sendHelper');

auth.initialize(config.get('CRAWLER_SERVICE_AUTH_TOKEN') || 'secret', config.get('CRAWLER_SERVICE_FORCE_AUTH'));
mockInsights.setup(config.get('CRAWLER_INSIGHTS_KEY') || 'mock', true);
const mode = config.get('CRAWLER_MODE') || '';
const service = CrawlerFactory.createService(mode);
const app = express();

app.use(logger('dev'));
app.use(sendHelper());

// If we should be listening for webhooks, add the route before the json body parser so we get the raw bodies.
// Note also that the GitHub doc says events are capped at 5mb
app.use('/webhook', bodyParser.raw({ limit: '5mb', type: '*/*' }), require('./routes/webhook')(service, config.get('CRAWLER_WEBHOOK_SECRET')));
// It's safe to set limitation to 2mb.
app.use(bodyParser.json({ limit: '2mb' }));
app.use('/status', require('./routes/status')(service));
app.use('/config', require('./routes/config')(service));
app.use('/requests', require('./routes/requests')(service));
app.use('/queues', require('./routes/queues')(service));
app.use('/deadletters', require('./routes/deadletters')(service));

// to keep AlwaysOn flooding logs with errors
app.get('/', function (request, response, next) {
  response.helpers.send.noContent();
});

// Catch 404 and forward to error handler
const requestHandler = function (request, response, next) {
  let error = { message: 'Not Found' };
  error.status = 404;
  error.success = false;
  next(error);
};
app.use(requestHandler);

// Hang the service init code off a route middleware.  Doesn't really matter which one.
requestHandler.init = (app, callback) => {
  service.ensureInitialized().then(
    () => {
      service.run();
      console.log('Service initialized');
      // call the callback but with no args.  An arg indicates an error.
      callback();
    },
    error => {
      console.log(`Service initialization error: ${error.message}`);
      console.dir(error);
      callback(error);
    });
};

// Error handlers
const handler = function (error, request, response, next) {
  appInsights.client.trackException(error, { name: 'SvcRequestFailure' });
  if (response.headersSent) {
    return next(error);
  }
  response.status(error.status || 500);
  let propertiesToSerialize = ['success', 'message'];
  if (app.get('env') !== 'production') {
    propertiesToSerialize.push('stack');
  }
  // Properties on Error object aren't enumerable so need to explicitly list properties to serialize
  response.send(JSON.stringify(error, propertiesToSerialize));
  response.end();
};
app.use(handler);

module.exports = app;
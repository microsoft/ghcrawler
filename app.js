// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

process.on('unhandledRejection', (reason, p) => {
  throw reason;
});

const appInsights = require('applicationinsights');
const auth = require('./middleware/auth');
const bodyParser = require('body-parser');
const config = require('painless-config');
const express = require('express');
const logger = require('morgan');
const sendHelper = require('./middleware/sendHelper');
const CrawlerFactory = require('./crawlerFactory');
const mockInsights = require('./providers/logger/mockInsights');
mockInsights.setup(config.get('CRAWLER_INSIGHTS_KEY') || 'mock', true);

function configureApp(service) {
  auth.initialize(config.get('CRAWLER_SERVICE_AUTH_TOKEN') || 'secret', config.get('CRAWLER_SERVICE_FORCE_AUTH'));

  const app = express();

  app.disable('x-powered-by');
  app.use(logger('dev'));
  app.use(sendHelper());

  // If we should be listening for webhooks, add the route before the json body parser so we get the raw bodies.
  // Note also that the GitHub doc says events are capped at 5mb
  app.use('/webhook', bodyParser.raw({ limit: '5mb', type: '*/*' }), require('./routes/webhook')(service, config.get('CRAWLER_WEBHOOK_SECRET')));
  // It's safe to set limitation to 2mb.
  app.use(bodyParser.json({ limit: '2mb', strict: false }));
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
    if (!(request && request.url && request.url.includes('robots933456.txt'))) // https://feedback.azure.com/forums/169385-web-apps/suggestions/32120617-document-healthcheck-url-requirement-for-custom-co
      appInsights.defaultClient.trackException({ exception: error, properties: { name: 'SvcRequestFailure' } });
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
  return app;
}

module.exports = configureApp;

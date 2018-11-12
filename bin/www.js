// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const appFactory = require('../app');
const config = require('painless-config');
const http = require('http');
const init = require('express-init');
const CrawlerFactory = require('../crawlerFactory');

function run(service, logger) {

  /**
   * Get port from environment and store in Express.
   */
  let port = normalizePort(config.get('CRAWLER_SERVICE_PORT') || process.env.PORT || '5000');
  port = port === 'random' ? null : port;

  if (!service) {
    // Create a default service. Seems unlikely that it will do much but could be fun
    const defaults = config.get('CRAWLER_OPTIONS') || './memoryConfig';
    service = CrawlerFactory.createService(require(defaults), logger);
  }
  const app = appFactory(service);
  app.set('port', port);

  const server = http.createServer(app);

  // initialize the apps (if they have async init functions) and start listening
  init(app, error => {
    if (error) {
      console.log('Error initializing the Express app: ' + error);
      throw new Error(error);
    }
    server.listen(port);
  });

  server.on('error', onError);
  server.on('listening', onListening);

  /**
   * Normalize a port into a number, string, or false.
   */

  function normalizePort(val) {
    const normalizedPort = parseInt(val, 10);

    if (isNaN(normalizedPort)) {
      // named pipe
      return val;
    }

    if (normalizedPort >= 0) {
      // port number
      return normalizedPort;
    }

    return false;
  }

  /**
   * Event listener for HTTP server 'error' event.
   */

  function onError(error) {
    if (error.syscall !== 'listen') {
      throw error;
    }

    const bind = typeof port === 'string'
      ? 'Pipe ' + port
      : 'Port ' + port;

    // handle specific listen errors with friendly messages
    switch (error.code) {
      case 'EACCES':
        console.error(bind + ' requires elevated privileges');
        process.exit(1);
        break;
      case 'EADDRINUSE':
        console.error(bind + ' is already in use');
        process.exit(1);
        break;
      default:
        throw error;
    }
  }

  /**
   * Event listener for HTTP server 'listening' event.
   */
  function onListening() {
    const addr = server.address();
    var bind = typeof addr === 'string'
      ? 'pipe ' + addr
      : 'port ' + addr.port;
    console.log(`Crawler service listening on ${bind}`);
  }
}

module.exports = run;

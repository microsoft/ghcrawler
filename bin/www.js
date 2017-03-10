// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const app = require('../app');
const config = require('painless-config');
const http = require('http');
const init = require('express-init');

/**
 * Get port from environment and store in Express.
 */
let port = normalizePort(config.get('CRAWLER_SERVICE_PORT') || process.env.PORT || '3000');
port = port === 'random' ? null : port;

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
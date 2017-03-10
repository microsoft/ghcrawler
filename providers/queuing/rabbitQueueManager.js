// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const AmqpQueue = require('./amqpQueue');
const AttenuatedQueue = require('./attenuatedQueue');
const Q = require('q');
const request = require('request');
const Request = require('ghcrawler').request;
const TrackedQueue = require('./trackedQueue');

class RabbitQueueManager {
  constructor(amqpUrl, managementEndpoint) {
    this.url = amqpUrl;
    this.managementEndpoint = managementEndpoint;
  }

  createQueueChain(name, tracker, options) {
    const formatter = message => {
      return Request.adopt(JSON.parse(message));
    };
    let queue = new AmqpQueue(this, name, formatter, options);
    if (tracker) {
      queue = new TrackedQueue(queue, tracker, options);
    }
    return new AttenuatedQueue(queue, options);
  }

  flushQueue(name) {
    return this._call('delete', `${this.managementEndpoint}/api/queues/%2f/${name}/contents`, `Could not flush queue ${name}`, false);
  }

  getInfo(name) {
    return this._call('get', `${this.managementEndpoint}/api/queues/%2f/${name}`, `Could not get info for queue ${name}`).then(info => {
      return { count: info.messages };
    });
  }

  _call(method, url, errorMessage, json = true, body = null) {
    const deferred = Q.defer();
    const options = {};
    if (json) {
      options.json = json;
    }
    if (body) {
      options.body = body;
    }
    request[method](url, options, (error, response, body) => {
      if (error || response.statusCode > 299) {
        const detail = error ? error.message : (typeof body === 'string' ? body : body.message);
        return deferred.reject(new Error(`${errorMessage}: ${detail}.`));
      }
      deferred.resolve(body);
    });
    return deferred.promise;
  }
}

module.exports = RabbitQueueManager;
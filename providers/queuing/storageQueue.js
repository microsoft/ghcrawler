// Copyright (c) Microsoft Corporation and others. Made available under the MIT license.
// SPDX-License-Identifier: MIT

const Q = require('q');
const qlimit = require('qlimit');

class StorageQueue {
  constructor(client, name, queueName, formatter, options) {
    this.client = client;
    this.name = name;
    this.queueName = queueName;
    this.messageFormatter = formatter;
    this.options = options;
    this.logger = options.logger;
  }

  subscribe() {
    const deferred = Q.defer();
    this.client.createQueueIfNotExists(this.queueName, (error) => {
      if (error) {
        return deferred.reject(error);
      }
      this.logger.info(`Subscribed to ${this.queueName} using Queue Storage`);
      deferred.resolve();
    });
    return deferred.promise;
  }

  unsubscribe() {
    return Q();
  }

  push(requests) {
    requests = Array.isArray(requests) ? requests : [requests];
    return Q.all(requests.map(qlimit(this.options.parallelPush || 1)(request => {
      const body = JSON.stringify(request);
      const deferred = Q.defer();
      this.client.createMessage(this.queueName, body, (error) => {
        if (error) {
          return deferred.reject(error);
        }
        this._incrementMetric('push');
        this._log('Queued', request);
        deferred.resolve();
      });
      return deferred.promise;
    })));
  }

  pop() {
    const msgOptions = { numOfMessages: 1, visibilityTimeout: this.options.visibilityTimeout || 60 * 60 };
    const deferred = Q.defer();
    this.client.getMessages(this.queueName, msgOptions, (error, result) => {
      if (error) {
        return deferred.reject(error);
      }
      this._incrementMetric('pop');
      const message = result[0];
      if (!message) {
        this.logger.verbose('No messages to receive');
        return deferred.resolve(null);
      }
      message.body = JSON.parse(message.messageText);
      const request = this.messageFormatter(message);
      request._message = message;
      this._log('Popped', message.body);
      deferred.resolve(request);
    });
    return deferred.promise;
  }

  done(request) {
    if (!request || !request._message) {
      return Q();
    }
    const deferred = Q.defer();
    this.client.deleteMessage(this.queueName, request._message.messageId, request._message.popReceipt, (error) => {
      if (error) {
        return deferred.reject(error);
      }
      this._incrementMetric('done');
      this._log('ACKed', request._message.body);
      deferred.resolve();
    });
    return deferred.promise;
  }

  defer(request) {
    this._incrementMetric('defer');
    return this.abandon(request);
  }

  abandon(request) {
    if (!request || !request._message) {
      return Q();
    }
    const deferred = Q.defer();
    // visibilityTimeout is updated to 0 to unlock/unlease the message
    this.client.updateMessage(this.queueName, request._message.messageId, request._message.popReceipt, 0, (error) => {
      if (error) {
        return deferred.reject(error);
      }
      this._incrementMetric('abandon');
      this._log('NAKed', request._message.body);
      deferred.resolve();
    });
    return deferred.promise;
  }

  flush() {
    const deleteQueue = Q.nbind(this.client.deleteQueue, this.client);
    const createQueueIfNotExists = Q.nbind(this.client.createQueueIfNotExists, this.client);
    return deleteQueue(this.queueName).then(createQueueIfNotExists(this.queueName));
  }

  getInfo() {
    const getQueueMetadata = Q.nbind(this.client.getQueueMetadata, this.client);
    return getQueueMetadata(this.queueName).then(result => {
      return Q({ count: result[0].approximateMessageCount });
    }).catch(error => {
      this.logger.error(error);
      return Q(null);
    });
  }

  getName() {
    return this.name;
  }

  _incrementMetric(operation) {
    const metrics = this.logger.metrics;
    if (metrics && metrics[this.name] && metrics[this.name][operation]) {
      metrics[this.name][operation].incr();
    }
  }

  _log(actionMessage, message) {
    this.logger.verbose(`${actionMessage} ${message.type} ${message.url}`);
  }
}

module.exports = StorageQueue;

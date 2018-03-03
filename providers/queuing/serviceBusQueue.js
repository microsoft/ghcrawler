// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const Q = require('q');
const qlimit = require('qlimit');

class ServiceBusQueue {
  constructor(client, name, queueName, formatter, manager, options) {
    this.client = client;
    this.name = name;
    this.queueName = queueName;
    this.messageFormatter = formatter;
    this.manager = manager;
    this.options = options;
    this.logger = options.logger;
  }

  subscribe() {
    this.logger.info(`Subscribed to ${this.queueName} using Service Bus`);
    return this.manager.createQueue(this.queueName, this.options);
  }

  unsubscribe() {
    return Q();
  }

  push(requests) {
    requests = Array.isArray(requests) ? requests : [requests];
    return Q.all(requests.map(qlimit(this.options.parallelPush || 1)(request => {
      const body = JSON.stringify(request);
      const deferred = Q.defer();
      this.client.sendQueueMessage(this.queueName, body, (error) => {
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
    const deferred = Q.defer();
    this.client.receiveQueueMessage(this.queueName, { isPeekLock: true }, (error, message) => {
      if (error === 'No messages to receive') {
        this.logger.verbose(error);
        return deferred.resolve(null);
      }
      if (error) {
        return deferred.reject(new Error(error));
      }
      this._incrementMetric('pop');
      message.body = JSON.parse(message.body);
      const request = this.messageFormatter(message);
      request._message = message;
      this._log('Popped', message.body);
      this._setLockRenewalTimer(message, 0, this.options.lockRenewal || 4.75 * 60 * 1000);
      deferred.resolve(request);
    });
    return deferred.promise;
  }

  done(request) {
    if (!request || !request._message) {
      return Q();
    }
    const deferred = Q.defer();
    this.client.deleteMessage(request._message, (error) => {
      if (error) {
        return deferred.reject(error);
      }
      this._incrementMetric('done');
      this._log('ACKed', request._message.body);
      clearTimeout(request._message._timeoutId);
      deferred.resolve();
    });
    return deferred.promise;
  }

  defer(request) {
    this._incrementMetric('defer');
    return this.abandon(request);
  }

  abandon(request) {
    const deferred = Q.defer();
    this.client.unlockMessage(request._message, (error) => {
      if (error) {
        return deferred.reject(error);
      }
      this._incrementMetric('abandon');
      this._log('NAKed', request._message.body);
      clearTimeout(request._message._timeoutId);
      deferred.resolve();
    });
    return deferred.promise;
  }

  flush() {
    return this.manager.flushQueue(this.queueName).then(() => this);
  }

  getInfo() {
    return this.manager.getInfo(this.queueName).then(info => {
      if (!info) {
        return null;
      }
      info.metricsName = `${this.options.queueName}:${this.name}`;
      return info;
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

  _setLockRenewalTimer(message, attempts = 0, delay = 4.5 * 60 * 1000) {
    attempts++;
    const timeoutId = setTimeout(() => {
      this.client.renewLockForMessage(message, (renewLockError) => {
        if (renewLockError) {
          this.logger.error(renewLockError);
        }
        this.logger.verbose(`Renewed lock on ${message.body.type} ${message.body.url}, attempt ${attempts}`);
        message._renewLockAttemptCount = attempts;
        this._setLockRenewalTimer(message, attempts, delay);
      });
    }, delay);
    message._timeoutId = timeoutId;
  }

  _log(actionMessage, message) {
    const attemptString = message && message._renewLockAttemptCount ? ` (renew lock attempt ${message._renewLockAttemptCount})` : '';
    this.logger.verbose(`${actionMessage} ${message.type} ${message.url}${attemptString}`);
  }
}

module.exports = ServiceBusQueue;

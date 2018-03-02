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
        return Q();
      }
      if (error) {
        return deferred.reject(new Error(error));
      }
      this._incrementMetric('pop');
      const request = this.messageFormatter(JSON.parse(message.body));
      request._message = message;
      this._log('Popped', request);
      this._setLockRenewalTimer(request, 0, this.options.lockRenewal || 4.75 * 60 * 1000);
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
      this._log('ACKed', request);
      clearTimeout(request._timeoutId);
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
      this._log('NAKed', request);
      clearTimeout(request._timeoutId);
      deferred.resolve();
    });
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

  _setLockRenewalTimer(request, attempts = 0, delay = 4.5 * 60 * 1000) {
    attempts++;
    const timeoutId = setTimeout(() => {
      this.client.renewLockForMessage(request._message, (renewLockError) => {
        if (renewLockError) {
          this.logger.error(renewLockError);
        }
        this.logger.verbose(`Renewed lock on ${request.type} ${request.url}, attempt ${attempts}`);
        request._renewLockAttemptCount = attempts;
        this._setLockRenewalTimer(request, attempts, delay);
      });
    }, delay);
    request._timeoutId = timeoutId;
  }

  _log(actionMessage, request) {
    const attemptString = request._renewLockAttemptCount ? ` (attempt ${request._renewLockAttemptCount})` : '';
    this.logger.verbose(`${actionMessage} ${request.type} ${request.url}${attemptString}`);
  }
}

module.exports = ServiceBusQueue;

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const amqp = require('amqplib');
const Q = require('q');
const qlimit = require('qlimit');

class AmqpQueue {
  constructor(manager, name, formatter, options) {
    this.manager = manager;
    this.messageFormatter = formatter;
    this.name = name;
    this.queueName = `${options.queueName}-${name}`;
    this.options = options;
    this.logger = options.logger;
    this.pending = [];
    this.channel = null;
  }

  subscribe() {
    if (this.channel && (this.channel.isFulfilled() || this.channel.isPending())) {
      return this.channel;
    }
    return this._reconnect();
  }

  _reconnect() {
    const url = this.manager.url;
    if (this.channel) {
      this.logger.warn(`Reconnecting ${this.queueName} using AMQP`);
    }
    // Create a channel promise that is (or will be) connected to the queue.
    const self = this;
    this.channel = Q
      .try(() => {
        const socketOptions = self.options.socketOptions || {};
        return amqp.connect(url, socketOptions);
      })
      .then(connection => {
        connection.on('error', self._reconnect.bind(self));
        process.once('SIGINT', function () { connection.close(); });
        return connection.createConfirmChannel().then(channel => {
          channel.on('error', self._reconnect.bind(self));
          return channel.assertQueue(self.queueName, { durable: true }).then(() => channel);
        });
      })
      .then(
      channel => {
        this.logger.info(`Connected ${this.queueName} using AMQP`);
        return channel;
      },
      error => {
        this.logger.warn(`Reconnection failed for ${this.queueName} using AMQP`, error);
        return Q.delay(5000).then(self._reconnect.bind(self));
      });
    return this.channel;
  }

  unsubscribe() {
    if (this.channel) {
      this.channel.then(channel => { return channel.close(); });
    }
    return Q();
  }

  push(requests) {
    requests = Array.isArray(requests) ? requests : [requests];
    return Q.all(requests.map(qlimit(this.options.parallelPush || 1)(request => {
      const body = JSON.stringify(request);
      const deferred = Q.defer();
      this.channel.then(channel => {
        channel.sendToQueue(this.queueName, new Buffer(body), {}, (err, ok) => {
          if (err) {
            return deferred.reject(err);
          }
          this._incrementMetric('push');
          const attemptString = request.attemptCount ? ` (attempt ${request.attemptCount})` : '';
          this.logger.verbose(`Queued ${request.policy.getShortForm()} ${request.toString()}${attemptString}`);
          deferred.resolve();
        });
      });
      return deferred.promise;
    })));
  }

  pop() {
    const self = this;
    const message = this.pending.shift();
    if (message) {
      return Q(this._handleMessage(message));
    }
    return this.channel.then(channel => {
      return channel.get(self.queueName).then(response => {
        if (!response) {
          return null;
        }
        return this._handleMessage(response);
      });
    });
  }

  _handleMessage(response) {
    this._incrementMetric('pop');
    const message = new Buffer(response.content).toString();
    const request = this.messageFormatter(message);
    request._message = response;
    return request;
  }

  done(request) {
    if (request._message) {
      // delete the message so a subsequent abandon or done does not retry the ack/nak
      this._incrementMetric('done');
      const message = request._message;
      delete request._message;
      this._silly(`ACKed: ${request.type} ${request.url}`);
      // ACK and don't worry if it fails. The request will go back on the queue and be processed again.
      this.channel.then(channel => channel.ack(message));
    }
    return Q();
  }

  defer(request) {
    if (request._message) {
      this._incrementMetric('defer');
      // TODO this is not likely enough.  See the code in the amqp10 queue
      this.pending.push(request._message);
      delete request._message;
      this._silly(`Deferred: ${request.type} ${request.url}`);
    }
    return Q();
  }

  abandon(request) {
    if (request._message) {
      // delete the message so a subsequent abandon or done does not retry the ack/nak
      this._incrementMetric('abandon');
      const message = request._message;
      delete request._message;
      this._silly(`NAKed: ${request.type} ${request.url}`);
      this.channel.then(channel => channel.nack(message));
    }
    return Q();
  }

  flush() {
    return this.manager.flushQueue(this.queueName).then(() => this);
  }

  getInfo() {
    return this.manager.getInfo(this.queueName).then(info => {
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

  _silly(message) {
    if (this.logger) {
      this.logger.silly(message);
    }
  }
}

module.exports = AmqpQueue;
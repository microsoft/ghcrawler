// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const amqp10 = require('amqp10');
const Amqp10Queue = require('./amqp10Queue');
const AttenuatedQueue = require('./attenuatedQueue');
const azureCommon = require('azure-common');
const InMemoryRateLimiter = require('../limiting/inmemoryRateLimiter');
const RateLimitedPushQueue = require('./ratelimitedPushQueue');
const Request = require('ghcrawler').request;
const serviceBus = require('azure-sb');
const TrackedQueue = require('./trackedQueue');
const Q = require('q');


const AmqpClient = amqp10.Client;
const AmqpPolicy = amqp10.Policy;

class ServiceBusQueueManager {
  constructor(amqpUrl, managementEndpoint) {
    this.amqpUrl = amqpUrl;
    this.managementEndpoint = managementEndpoint;
    this.client = null;
    const retryOperations = new azureCommon.ExponentialRetryPolicyFilter();
    this.serviceBusService = serviceBus.createServiceBusService(managementEndpoint).withFilter(retryOperations);
  }

  createQueueClient(name, formatter, options) {
    return this._createClient(name, `${options.queueName}-${name}`, formatter, options);
  }

  createSubscriptionClient(name, topic, subscription, formatter, options) {
    return this._createClient(name, `${topic}/Subscriptions/${subscription}`, formatter, options);
  }

  _createClient(name, queueName, formatter, options) {
    return new Amqp10Queue(this._getClient(), name, queueName, formatter, this, options);
  }

  _getClient() {
    if (this.client) {
      return this.client;
    }
    const actualClient = new AmqpClient(AmqpPolicy.ServiceBusQueue);
    this.client = actualClient.connect(this.amqpUrl).then(() => { return actualClient; });
    return this.client;
  }

  createQueueChain(name, tracker, options) {
    const formatter = message => {
      // make sure the message/request object is copied to enable deferral scenarios (i.e., the request is modified
      // and then put back on the in-memory queue)
      return Request.adopt(Object.assign({}, message.body));
    };
    let queue = this.createQueueClient(name, formatter, options);
    if (tracker) {
      queue = new TrackedQueue(queue, tracker, options);
    }
    if (options.pushRateLimit) {
      const limiter = InMemoryRateLimiter.create({
        key: () => 'queue:' + name,
        window: () => options.pushRateWindow || 2,
        limit: () => options.pushRateLimit || 300
      });

      queue = new RateLimitedPushQueue(queue, limiter, options);
    }
    return new AttenuatedQueue(queue, options);
  }

  flushQueue(name) {
    return Q()
      .then(this.deleteQueue.bind(this, name))
      .then(this.createQueue.bind(this, name));
  }

  deleteQueue(name) {
    const deferred = Q.defer();
    this.serviceBusService.deleteQueue(name, error => {
      if (error) {
        return deferred.reject(error);
      }
      deferred.resolve();
    });
    return deferred.promise;
  }

  createQueue(name) {
    const options = {
      EnablePartitioning: true,
      LockDuration: 'PT5M',
      DefaultMessageTimeToLive: 'P10675199D',
      MaxDeliveryCount: '10000000'
    };
    const deferred = Q.defer();
    this.serviceBusService.createQueueIfNotExists(name, options, (error, created, response) => {
      if (error) {
        return deferred.reject(error);
      }
      deferred.resolve(response.body);
    });
    return deferred.promise;
  }

  getInfo(name) {
    // TODO see if this works for subscriptions?
    const deferred = Q.defer();
    this.serviceBusService.getQueue(name, (error, queue) => {
      if (error) {
        if (error.code === 'QueueNotFound') {
          return deferred.resolve(null);
        }
        return deferred.reject(error);
      }
      // length of queue (active messages ready to read)
      let activeMessageCount;
      try {
        activeMessageCount = queue.CountDetails['d2p1:ActiveMessageCount'];
      } catch (e) {
        activeMessageCount = 0;
      }
      deferred.resolve({ count: activeMessageCount });
    });
    return deferred.promise;
  }
}

module.exports = ServiceBusQueueManager;
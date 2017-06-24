// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.const redlock = require('redlock');

const config = require('painless-config');
const Amqp10Queue = require('../providers/queuing/Amqp10Queue');
const AttenuatedQueue = require('../providers/queuing/AttenuatedQueue');
const InMemoryCrawlQueue = require('../providers/queuing/InMemoryCrawlQueue');
const RabbitQueueManager = require('../providers/queuing/RabbitQueueManager');
const RedisRequestTracker = require('../providers/queuing/RedisRequestTracker');
const ServiceBusQueueManager = require('../providers/queuing/ServiceBusQueueManager');
const redisUtil = require('./util/redis');
const request = require('request');
const Request = require('../Request');
const moment = require('moment');
const QueueSet = require('../providers/queuing/QueueSet');
const lockingFactory = require('./locking-factory');
const amqp10 = require('amqp10');
const AmqpClient = amqp10.Client;
const AmqpPolicy = amqp10.Policy;

class EventFormatter {
  constructor(options) {
    this.options = options;
    this.logger = options.logger;
  }

  format(message) {
    // The message here is expected to be a WEBHOOK event.  Use the information included to identify the
    // repo or org to poll for new events.
    const event = message.body;
    const eventsUrl = event.repository ? event.repository.events_url : event.organization.events_url;
    const result = new Request('event_trigger', `${eventsUrl}`);
    result.payload = { body: event, etag: 1, fetchedAt: moment.utc().toISOString() };
    // requests off directly off the event feed do not need exclusivity
    request.requiresLock = false;
    // if the event is for a private repo, mark the request as needing private access.
    if (event.repository && event.repository.private) {
      request.context.repoType = 'private';
    }
    // mark it to be retried on the immediate queue as we don't want to requeue it on this shared topic
    request._retryQueue = 'immediate';
    return request;
  }
}

function createRequestTracker(prefix, options) {
  let locker = lockingFactory.createLocker(
    options.tracker.locking || { provider: 'memory'});
  return new RedisRequestTracker(prefix, redisUtil.getRedisClient(options.logger), locker, options);
}

function createAmqpQueues(options) {
  const managementEndpoint = config.get('CRAWLER_RABBIT_MANAGER_ENDPOINT');
  const url = config.get('CRAWLER_AMQP_URL');
  const manager = new RabbitQueueManager(url, managementEndpoint, options.socketOptions ? options.socketOptions.ca : null);
  const env = process.env.NODE_ENV;
  const tracker = createRequestTracker(`${env}:AMQP:${options.queueName}`, options);
  return createQueueSet(manager, tracker, options);
}

function createAmqp10Queues(options) {
  const managementEndpoint = config.get('CRAWLER_SERVICEBUS_MANAGER_ENDPOINT');
  const amqpUrl = config.get('CRAWLER_AMQP10_URL');
  const manager = new ServiceBusQueueManager(amqpUrl, managementEndpoint);
  const env = process.env.NODE_ENV;
  const tracker = createRequestTracker(`${env}:AMQP10:${options.queueName}`, options);
  return createQueueSet(manager, tracker, options);
}

function createMemoryQueues(options) {
  const manager = {
    createQueueChain: (name, tracker, options) => {
      return createMemoryQueue(name, options);
    }
  };
  return createQueueSet(manager, null, options);
}

function createQueueSet(manager, tracker, options) {
  const immediate = manager.createQueueChain('immediate', tracker, options);
  const soon = manager.createQueueChain('soon', tracker, options);
  const normal = manager.createQueueChain('normal', tracker, options);
  const later = manager.createQueueChain('later', tracker, options);
  const queues = addEventQueue(manager, [immediate, soon, normal, later], options);
  return new QueueSet(queues, options);
}

function createMemoryQueue(name, options) {
  return new AttenuatedQueue(new InMemoryCrawlQueue(name, options), options);
}

function addEventQueue(manager, queues, options) {
  if (options.events.provider && options.events.provider !== 'none') {
    queues.unshift(createEventQueue(manager, options));
  }
  return queues;
}

function createAmqp10EventSubscription(options) {
  const amqpUrl = config.get('CRAWLER_EVENT_AMQP10_URL');
  const actualClient = new AmqpClient(AmqpPolicy.ServiceBusQueue);
  const client = actualClient.connect(amqpUrl).then(() => { return actualClient; });
  const formatter = new EventFormatter(options);
  const queueName = `${options.events.topic}/Subscriptions/${options.events.queueName}`;
  const result = new Amqp10Queue(client, 'events', queueName, formatter.format.bind(formatter), null, options);
  result.mode = { receive: 'receive' };
  return result;
}

function createEventQueue(manager, options) {
  if (options.events.provider === 'amqp10') {
    return createAmqp10EventSubscription(options);
  }
  if (options.events.provider === 'webhook') {
    return manager.createQueueChain('events', null, options);
  }
  throw new Error(`No event provider for ${options.events.provider}`);
}

function createQueues(options) {
  const provider = options.provider || 'amqp10';
  if (provider === 'amqp10') {
    return createAmqp10Queues(options);
  } else if (provider === 'amqp') {
    return createAmqpQueues(options);
  } else if (provider === 'memory') {
    return createMemoryQueues(options);
  } else {
    throw new Error(`Invalid queue provider option: ${provider}`);
  }
}

exports.createQueues = createQueues;
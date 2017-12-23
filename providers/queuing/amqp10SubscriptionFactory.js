// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const RabbitQueueManager = require('./rabbitQueueManager');
const CrawlerFactory = require('../../CrawlerFactory');
const Amqp10Queue = require('./amqp10Queue');
const amqp10 = require('amqp10');
const AmqpClient = amqp10.Client;
const AmqpPolicy = amqp10.Policy;
const Request = require('../../lib/request');

// {
//   url: config.get('CRAWLER_EVENT_AMQP10_URL')
// }

module.exports = (manager, options) => {
  const { url } = options;
  const actualClient = new AmqpClient(AmqpPolicy.ServiceBusQueue);
  const client = actualClient.connect(url).then(() => { return actualClient; });
  const formatter = new EventFormatter(options);
  const queueName = `${options.events.topic}/Subscriptions/${options.events.queueName}`;
  const result = new Amqp10Queue(client, 'events', queueName, formatter.format.bind(formatter), null, options);
  result.mode = { receive: 'receive' };
  return result;
}

class EventFormatter {
  constructor(options) {
    this.options = options;
    this.logger = options.logger;
  }

  format(message) {
    // The message here is expected to be a WEBHOOK event.  Use the information included to identify the
    // repo or org to poll for new events.
    const type = message.applicationProperties.event;
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

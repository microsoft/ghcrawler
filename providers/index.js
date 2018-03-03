// Copyright (c) Microsoft Corporation and others. Made available under the MIT license.
// SPDX-License-Identifier: MIT

module.exports = {
  queue: {
    amqp: require('./queuing/amqpFactory'),
    amqp10: require('./queuing/amqp10Factory'),
    serviceBus: require('./queuing/serviceBusFactory'),
    storageQueueFactory: require('./queuing/storageQueueFactory'),
    memory: require('./queuing/memoryFactory'),
    amqp10Subscription: require('./queuing/amqp10SubscriptionFactory'),
    webhook: require('./queuing/webhookFactory')
  },
  store: {
    memory: require('./storage/inmemoryDocStore'),
    file: require('./storage/file'),
    mongo: require('./storage/mongodocstore'),
    azblob: require('./storage/azureBlobFactory')
  },
  lock: {
    memory: require('./locker/memory'),
    redlock: require('./locker/redlock')
  },
  redis: {
    redis: require('./redis/redisFactory')
  }
};

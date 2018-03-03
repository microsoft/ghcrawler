// Copyright (c) Microsoft Corporation and others. Made available under the MIT license.
// SPDX-License-Identifier: MIT

const AttenuatedQueue = require('./attenuatedQueue');
const AzureStorage = require('azure-storage');
const Request = require('../../lib/request');
const StorageQueue = require('./storageQueue');
const TrackedQueue = require('./trackedQueue');

class StorageQueueManager {
  constructor(connectionString) {
    const retryOperations = new AzureStorage.ExponentialRetryPolicyFilter();
    this.client = AzureStorage.createQueueService(connectionString).withFilter(retryOperations);
  }

  createQueueClient(name, formatter, options) {
    return new StorageQueue(this.client, name, `${options.queueName}-${name}`, formatter, options);
  }

  createQueueChain(name, tracker, options) {
    const formatter = message => {
      // make sure the message/request object is copied to enable deferral scenarios (i.e., the request is modified
      // and then put back on the queue)
      return Request.adopt(Object.assign({}, message.body));
    };
    let queue = this.createQueueClient(name, formatter, options);
    if (tracker) {
      queue = new TrackedQueue(queue, tracker, options);
    }
    return new AttenuatedQueue(queue, options);
  }
}

module.exports = StorageQueueManager;

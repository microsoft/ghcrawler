// Copyright (c) Microsoft Corporation and others. Made available under the MIT license.
// SPDX-License-Identifier: MIT

// Run ./node_modules/mocha/bin/mocha test/integration/storageQueueTests.js --timeout 40000
const config = require('painless-config');
const { expect } = require('chai');
const { after, before, describe, it } = require('mocha');
const { promisify } = require('util');
const Request = require('../../lib/request');
const StorageQueue = require('../../providers/queuing/storageQueue');
const StorageQueueManager = require('../../providers/queuing/storageQueueManager');

const connectionString = config.get('AZQUEUE_CONNECTION_STRING');
const name = config.get('CRAWLER_NAME');
const queueName = 'storage-queue-test';
const formatter = message => {
  Request.adopt(message);
  return message;
};
const options = {
  logger: {
    info: console.log,
    verbose: console.log,
    error: console.error
  },
  queueName,
  visibilityTimeout: 3 // in sec
};
let storageQueue = null;

describe('Azure Storage Queue Integration', () => {
  before(async () => {
    if (!connectionString) {
      throw new Error('Storage Queue connectionString not configured.');
    }
    const manager = new StorageQueueManager(connectionString);
    storageQueue = new StorageQueue(manager.client, name, queueName, formatter, options);
    await storageQueue.subscribe();
  });

  // after(async () => {
  //   await storageQueue.flush();
  // });

  it('Should push, pop and ack a message, pop empty queue', async () => {
    let info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(0);
    const msg = new Request('test1', 'test://test/test1');
    await storageQueue.push(msg);
    info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    let request = await storageQueue.pop();
    expect(request).to.exist;
    expect(request instanceof Request).to.be.true;
    info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    await storageQueue.done(request);
    info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(0);

    request = await storageQueue.pop();
    expect(request).to.be.null;
  });

  it('Should push, pop, nack, pop and ack a message', async () => {
    let info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(0);
    const msg = new Request('test2', 'test://test/test2');
    await storageQueue.push(msg);
    info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    let request = await storageQueue.pop();
    expect(request).to.exist;
    expect(request instanceof Request).to.be.true;
    info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    await storageQueue.abandon(request);
    info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    request = await storageQueue.pop();
    expect(request).to.exist;
    expect(request instanceof Request).to.be.true;
    info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    await storageQueue.done(request);
    info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(0);
  });

  it('Should push, pop, wait for message to be unlocked, pop and ack a message', async () => {
    let info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(0);
    const msg = new Request('test3', 'test://test/test3');
    await storageQueue.push(msg);
    info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    let request = await storageQueue.pop();
    expect(request).to.exist;
    expect(request instanceof Request).to.be.true;
    info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    await setTimeout[promisify.custom](4000);
    info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    request = await storageQueue.pop();
    expect(request).to.exist;
    expect(request instanceof Request).to.be.true;
    info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    await storageQueue.done(request);
    info = await storageQueue.getInfo();
    expect(Number(info.count)).to.equal(0);
  });
});

function getTimerAsyncId(timeoutId) {
  return timeoutId[Object.getOwnPropertySymbols(timeoutId)[0]];
}
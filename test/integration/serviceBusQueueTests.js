// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

// Run ./node_modules/mocha/bin/mocha test/integration/serviceBusQueueTests.js --timeout 60000
const config = require('painless-config');
const { expect } = require('chai');
const { after, before, describe, it } = require('mocha');
const { promisify } = require('util');
const Request = require('../../lib/request');
const ServiceBusQueue = require('../../providers/queuing/serviceBusQueue');
const ServiceBusQueueManager = require('../../providers/queuing/serviceBusQueueManager');

const connectionString = config.get('CRAWLER_SERVICEBUS_CONNECTION_STRING') || config.get('CRAWLER_SERVICEBUS_MANAGER_ENDPOINT');
const name = config.get('CRAWLER_NAME');
const queueName = 'sb-test';
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
  enablePartitioning: 'false',
  maxSizeInMegabytes: '5120',
  lockDuration: 'PT4S', // 4 sec
  lockRenewal: 3000, // 3 sec
  maxDeliveryCount: 100,
  _config: { on: () => { } }
};
let serviceBusQueue = null;

describe('Service Bus Integration', () => {
  before(async () => {
    if (!connectionString) {
      throw new Error('ServiceBus connectionString not configured.');
    }
    const manager = new ServiceBusQueueManager(null, connectionString, true);
    serviceBusQueue = new ServiceBusQueue(manager.serviceBusService, name, queueName, formatter, manager, options);
    await serviceBusQueue.subscribe();
  });

  after(async () => {
    await serviceBusQueue.flush();
  });

  it('Should push, pop and ack a message', async () => {
    let info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(0);
    const msg = new Request('test1', 'test://test/test1');
    await serviceBusQueue.push(msg);
    info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    const request = await serviceBusQueue.pop();
    expect(request).to.exist;
    expect(request instanceof Request).to.be.true;
    expect(request._timeoutId).to.exist;
    info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    await serviceBusQueue.done(request);
    info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(0);
  });

  it('Should push, pop, nack, pop and ack a message', async () => {
    let info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(0);
    const msg = new Request('test2', 'test://test/test2');
    await serviceBusQueue.push(msg);
    info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    let request = await serviceBusQueue.pop();
    expect(request).to.exist;
    expect(request instanceof Request).to.be.true;
    expect(request._timeoutId).to.exist;
    info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    await serviceBusQueue.abandon(request);
    info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    request = await serviceBusQueue.pop();
    expect(request).to.exist;
    expect(request instanceof Request).to.be.true;
    expect(request._timeoutId).to.exist;
    info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    await serviceBusQueue.done(request);
    info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(0);
  });

  it('Should push, pop, wait for lock to be renewed and ack a message', async () => {
    let info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(0);
    const msg = new Request('test3', 'test://test/test3');
    await serviceBusQueue.push(msg);
    info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    let request = await serviceBusQueue.pop();
    expect(request).to.exist;
    expect(request instanceof Request).to.be.true;
    expect(request._timeoutId).to.exist;
    expect(request._message.brokerProperties.LockedUntilUtc).to.exist;
    expect(new Date(request._message.brokerProperties.LockedUntilUtc)).to.be.greaterThan(new Date());
    info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(1);

    expect(request._renewLockAttemptCount).to.be.undefined;
    const timerAsyncId = getTimerAsyncId(request._timeoutId);
    await setTimeout[promisify.custom](4000);
    expect(request._renewLockAttemptCount).to.equal(1);
    expect(getTimerAsyncId(request._timeoutId)).not.to.be.equal(timerAsyncId);

    await serviceBusQueue.done(request);
    info = await serviceBusQueue.getInfo();
    expect(Number(info.count)).to.equal(0);
  });
});

function getTimerAsyncId(timeoutId) {
  return timeoutId[Object.getOwnPropertySymbols(timeoutId)[0]];
}
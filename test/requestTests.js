const assert = require('chai').assert;
const chai = require('chai');
const expect = require('chai').expect;
const Request = require('../lib/request.js');
const sinon = require('sinon');

describe('Request transitivity', () => {
  it('will not queueRoot if none transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'none';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueRoot('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(0);
  });

  it('will queueRoot normal if normal transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'normal';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueRoot('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(1);
    expect(request.crawler.queue.getCall(0).args[0].transitivity).to.be.equal('normal');
  });

  it('will not queueRoot if forceNone transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'forceNone';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueRoot('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(0);
  });

  it('will queueRoot normal if forceNormal transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'forceNormal';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueRoot('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(1);
    expect(request.crawler.queue.getCall(0).args[0].transitivity).to.be.equal('normal');
  });

  it('will queueRoot forceNormal if forceForce transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'forceForce';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueRoot('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(1);
    expect(request.crawler.queue.getCall(0).args[0].transitivity).to.be.equal('forceNormal');
  });

  it('will not queueChild if none transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'none';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueChild('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(0);
  });

  it('will queueChild normal if normal transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'normal';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueChild('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(1);
    expect(request.crawler.queue.getCall(0).args[0].transitivity).to.be.equal('normal');
  });

  it('will queueChild force if force transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'forceNone';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueChild('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(1);
    expect(request.crawler.queue.getCall(0).args[0].transitivity).to.be.equal('forceNone');
  });

  it('will queueChild foceNormal if forceNormal transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'forceNormal';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueChild('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(1);
    expect(request.crawler.queue.getCall(0).args[0].transitivity).to.be.equal('forceNormal');
  });

  it('will queueChild foceNormal if forceForce transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'forceForce';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueChild('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(1);
    expect(request.crawler.queue.getCall(0).args[0].transitivity).to.be.equal('forceNormal');
  });
});

describe('Request context/qualifier', () => {
  it('will not queueRoot if none transitivity', () => {
  });
});

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const assert = require('chai').assert;
const expect = require('chai').expect;
const Request = require('../lib/request.js');
const sinon = require('sinon');
const TraversalPolicy = require('../lib/traversalPolicy');

describe('Request transitivity', () => {

  it('will queue contains relationship correctly for broad transitivity', () => {
    let request = new Request('user', 'http://test.com/users/user1');
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');

    request.relationship = 'contains';
    request.queue('contains', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(1);
    expect(request.crawler.queue.getCall(0).args[0].policy.transitivity).to.be.equal('broad');

    request.relationship = 'belongsTo';
    request.queue('contains', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(2);
    expect(request.crawler.queue.getCall(1).args[0].policy.transitivity).to.be.equal('only');

    request.relationship = 'isa';
    request.queue('contains', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(3);
    expect(request.crawler.queue.getCall(2).args[0].policy.transitivity).to.be.equal('only');

    request.relationship = 'reference';
    request.queue('contains', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(4);
    expect(request.crawler.queue.getCall(3).args[0].policy.transitivity).to.be.equal('only');
  });

  it('will queue belongsTo relationship correctly for broad transitivity', () => {
    let request = new Request('user', 'http://test.com/users/user1');
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');

    request.relationship = 'contains';
    request.queue('belongsTo', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(1);
    expect(request.crawler.queue.getCall(0).args[0].policy.transitivity).to.be.equal('only');

    request.relationship = 'belongsTo';
    request.queue('belongsTo', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(2);
    expect(request.crawler.queue.getCall(1).args[0].policy.transitivity).to.be.equal('broad');

    request.relationship = 'isa';
    request.queue('belongsTo', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(3);
    expect(request.crawler.queue.getCall(2).args[0].policy.transitivity).to.be.equal('only');

    request.relationship = 'reference';
    request.queue('belongsTo', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(4);
    expect(request.crawler.queue.getCall(3).args[0].policy.transitivity).to.be.equal('only');
  });

  it('will queue isa relationship correctly for broad transitivity', () => {
    let request = new Request('user', 'http://test.com/users/user1');
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');

    request.relationship = 'contains';
    request.queue('isa', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(1);
    expect(request.crawler.queue.getCall(0).args[0].policy.transitivity).to.be.equal('only');

    request.relationship = 'belongsTo';
    request.queue('isa', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(2);
    expect(request.crawler.queue.getCall(1).args[0].policy.transitivity).to.be.equal('only');

    request.relationship = 'isa';
    request.queue('isa', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(3);
    expect(request.crawler.queue.getCall(2).args[0].policy.transitivity).to.be.equal('only');

    request.relationship = 'reference';
    request.queue('isa', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(4);
    expect(request.crawler.queue.getCall(3).args[0].policy.transitivity).to.be.equal('only');
  });

  it('will queue reference relationship correctly for broad transitivity', () => {
    let request = new Request('user', 'http://test.com/users/user1');
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');

    request.relationship = 'contains';
    request.queue('reference', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(1);
    expect(request.crawler.queue.getCall(0).args[0].policy.transitivity).to.be.equal('only');

    request.relationship = 'belongsTo';
    request.queue('reference', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(2);
    expect(request.crawler.queue.getCall(1).args[0].policy.transitivity).to.be.equal('only');

    request.relationship = 'isa';
    request.queue('reference', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(3);
    expect(request.crawler.queue.getCall(2).args[0].policy.transitivity).to.be.equal('only');

    request.relationship = 'reference';
    request.queue('reference', 'foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(4);
    expect(request.crawler.queue.getCall(3).args[0].policy.transitivity).to.be.equal('only');
  });
});

describe('Request context/qualifier', () => {
  it('will not queueRoot if none transitivity', () => {
  });
});

describe('Request link management', () => {
  it('will throw if no qualifier available', () => {
    const request = new Request('foo', 'http://test');
    try {
      request.addSelfLink();
      assert.fail();
    } catch (error) {
      expect(error).to.not.be.null;
    }
  });

  it('will add a : to the qualifier', () => {
    const request = new Request('foo', 'http://test');
    request.document = { id: 4, _metadata: { links: {} } };
    request.context.qualifier = 'test';
    request.addSelfLink();
    expect(request.document._metadata.links.self.href.startsWith('test:foo'));
  });
});

describe('Request promise management', () => {
  it('will track single promises', () => {
    const request = new Request('test', 'http://test');
    request.track('foo');
    expect(request.promises.length).to.be.equal(1);
    expect(request.promises[0]).to.be.equal('foo');
  });

  it('will track multiple promises', () => {
    const request = new Request('test', 'http://test');
    request.track(['foo', 'bar']);
    expect(request.promises.length).to.be.equal(2);
    expect(request.promises[0]).to.be.equal('foo');
    expect(request.promises[1]).to.be.equal('bar');
    request.track(['x', 'y']);
    expect(request.promises.length).to.be.equal(4);
    expect(request.promises[2]).to.be.equal('x');
    expect(request.promises[3]).to.be.equal('y');
  });
});

describe('Request marking', () => {
  it('will markSkip and preserve the first value', () => {
    const request = new Request('test', 'http://test');
    request.markSkip('foo', 'bar');
    expect(request.shouldSkip()).to.be.true;
    expect(request.outcome).to.be.equal('foo');
    expect(request.message).to.be.equal('bar');

    request.markSkip('x', 'y');
    expect(request.shouldSkip()).to.be.true;
    expect(request.outcome).to.be.equal('foo');
    expect(request.message).to.be.equal('bar');
  });

  it('will markSkip and preserve the first value even if not set', () => {
    const request = new Request('test', 'http://test');
    request.markSkip();
    expect(request.shouldSkip()).to.be.true;
    expect(request.outcome).to.be.undefined;
    expect(request.message).to.be.undefined;

    request.markSkip('x', 'y');
    expect(request.shouldSkip()).to.be.true;
    expect(request.outcome).to.be.undefined;
    expect(request.message).to.be.undefined;
  });

  it('will markRequeue and preserve the first value', () => {
    const request = new Request('test', 'http://test');
    request.markRequeue('foo', 'bar');
    expect(request.shouldRequeue()).to.be.true;
    expect(request.outcome).to.be.equal('foo');
    expect(request.message).to.be.equal('bar');

    request.markRequeue('x', 'y');
    expect(request.shouldRequeue()).to.be.true;
    expect(request.outcome).to.be.equal('foo');
    expect(request.message).to.be.equal('bar');
  });

  it('will markRequeue and preserve the first value even if not set', () => {
    const request = new Request('test', 'http://test');
    request.markRequeue();
    expect(request.shouldRequeue()).to.be.true;
    expect(request.outcome).to.be.undefined;
    expect(request.message).to.be.undefined;

    request.markRequeue('x', 'y');
    expect(request.shouldRequeue()).to.be.true;
    expect(request.outcome).to.be.undefined;
    expect(request.message).to.be.undefined;
  });

});

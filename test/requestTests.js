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

  it('will not queueRoots if none transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'none';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueRoots('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(0);
  });

  it('will not queueChild if none transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'none';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueChild('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(0);
  });

  it('will not queueChildren if none transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'none';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueChildren('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(0);
  });

  it('will queueRoot normal if normal transitivity', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'normal';
    request.fetch = 'none';
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueRoot('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(1);
    expect(request.crawler.queue.getCall(0).args[0].transitivity).to.be.equal('normal');
    expect(request.crawler.queue.getCall(0).args[0].fetch).to.be.equal('none');
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

  it('queueRoots will not change transitivity and will carry through fetch', () => {
    const request = new Request('user', 'http://test.com/users/user1');
    request.transitivity = 'forceForce';
    request.fetch = 'force';
    request.document = { _metadata: { links: { self: { href: 'urn:pick:me' } } } };
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    request.queueRoots('foo', 'http://');
    expect(request.crawler.queue.callCount).to.be.equal(1);
    const newRequest = request.crawler.queue.getCall(0).args[0];
    expect(newRequest.transitivity).to.be.equal('forceForce');
    expect(newRequest.fetch).to.be.equal('force');
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
    request.addSelfLink('id', 'test');
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

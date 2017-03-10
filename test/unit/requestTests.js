// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const assert = require('chai').assert;
const expect = require('chai').expect;
const Request = require('../../lib/request.js');
const sinon = require('sinon');
const TraversalPolicy = require('../../lib/traversalPolicy');

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

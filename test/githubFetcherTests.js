const assert = require('chai').assert;
const chai = require('chai');
const Crawler = require('../lib/crawler');
const expect = require('chai').expect;
const extend = require('extend');
const GitHubFetcher = require('../lib/githubFetcher');
const Q = require('q');
const QueueSet = require('../lib/queueSet');
const Request = require('../lib/request');
const sinon = require('sinon');
const TraversalPolicy = require('../lib/traversalPolicy');

describe('GitHub fetcher', () => {

  it('should fetch one unseen document', () => {
    const request = new Request('foo', 'http://test');
    const responses = [createResponse('test')];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const store = createBaseStore({ etag: () => { return Q(null); } });
    const fetcher = createBaseFetcher({ requestor: requestor, store: store });
    return fetcher.fetch(request).then(request => {
      expect(request.document).to.be.equal('test');
      expect(request.response.statusCode).to.be.equal(200);
      expect(request.shouldSkip()).to.be.false;
    });
  });

  it('should set proper types for collection requests', () => {
    const url = 'http://test';
    const request = new Request('repos', url);
    let etagArgs = null;
    let getArgs = null;
    const responses = [createResponse('test')];
    const requestor = createBaseRequestor({
      get: (url, options) => { getArgs = { url: url, options: options }; return Q(responses.shift()); }
    });
    const store = createBaseStore({
      etag: (type, url) => { etagArgs = { type: type, url: url }; return Q(null); },
    });
    const fetcher = createBaseFetcher({ requestor: requestor, store: store });
    return fetcher.fetch(request).then(request => {
      expect(request.document).to.be.equal('test');
      expect(request.response.statusCode).to.be.equal(200);
      expect(request.shouldSkip()).to.be.false;
      expect(request.type).to.be.equal('repos');
      expect(etagArgs.type).to.be.equal('repos');
      expect(etagArgs.url).to.be.equal(url);
      expect(getArgs.url).to.be.equal(url);
    });
  });

  it('should requeue and delay on 403 forbidden throttling', () => {
    const request = new Request('foo', 'http://test');
    const responses = [createResponse('test', 403)];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const store = createBaseStore({ etag: () => { return Q(null); } });
    const fetcher = createBaseFetcher({ requestor: requestor, store: store });
    return fetcher.fetch(request).then(request => {
      expect(request.document).to.be.undefined;
      expect(request.shouldRequeue()).to.be.true;
      expect(request.nextRequestTime > Date.now()).to.be.true;
    });
  });

  it('should delay on backoff throttling', () => {
    const request = new Request('foo', 'http://test');
    const resetTime = Date.now() + 2000;
    const responses = [createResponse('bar', 200, null, 30, resetTime)];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const store = createBaseStore({ etag: () => { return Q(null); } });
    const fetcher = createBaseFetcher({ requestor: requestor, store: store });
    return fetcher.fetch(request).then(request => {
      expect(request.document).to.be.equal('bar');
      expect(request.shouldRequeue()).to.be.false;
      expect(request.shouldSkip()).to.be.false;
      expect(request.nextRequestTime).to.be.equal(resetTime);
    });
  });

  it('should delay on Retry-After throttling', () => {
    const request = new Request('foo', 'http://test');
    const resetTime = Date.now() + 3000;
    const headers = { 'Retry-After': 3 };
    const responses = [createResponse('bar', 200, null, 30, resetTime, headers)];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const store = createBaseStore({ etag: () => { return Q(null); } });
    const fetcher = createBaseFetcher({ requestor: requestor, store: store });
    return fetcher.fetch(request).then(request => {
      expect(request.document).to.be.equal('bar');
      expect(request.shouldRequeue()).to.be.false;
      expect(request.shouldSkip()).to.be.false;
      // give at most 100ms for the test to run
      expect(request.nextRequestTime).to.be.within(resetTime, resetTime + 100);
    });
  });

  it('should skip 409s', () => {
    const request = new Request('foo', 'http://test');
    const responses = [createResponse('test', 409)];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const store = createBaseStore({ etag: () => { return Q(null); } });
    const fetcher = createBaseFetcher({ requestor: requestor, store: store });
    return fetcher.fetch(request).then(request => {
      expect(request.document).to.be.undefined;
      expect(request.shouldSkip()).to.be.true;
    });
  });

  it('should return cached content and not save and response for 304 with force', () => {
    const url = 'http://test';
    const request = new Request('repos', url);
    request.policy = TraversalPolicy.update();
    let getArgs = null;
    const responses = [createResponse(null, 304, 42)];
    const requestor = createBaseRequestor({
      get: (url, options) => { getArgs = { url: url, options: options }; return Q(responses.shift()); }
    });
    const store = createBaseStore({ etag: () => { return Q(42); }, get: () => { return Q({ _metadata: {}, id: 'test' }); } });
    const fetcher = createBaseFetcher({ requestor: requestor, store: store });
    return fetcher.fetch(request).then(request => {
      expect(request.document.id).to.be.equal('test');
      expect(request.response.statusCode).to.be.equal(304);
      expect(request.shouldSkip()).to.be.false;
      expect(request.contentOrigin).to.be.equal('cacheOfOrigin');
      expect(getArgs.options.headers['If-None-Match']).to.be.equal(42);
      expect(getArgs.url).to.be.equal(url);
    });
  });

  it('should return cached content and headers for 304 with force', () => {
    const url = 'http://test';
    const request = new Request('repos', url);
    request.policy = TraversalPolicy.update();
    let getArgs = null;
    const responses = [createResponse(null, 304, 42)];
    const requestor = createBaseRequestor({
      get: (url, options) => { getArgs = { url: url, options: options }; return Q(responses.shift()); }
    });
    const store = createBaseStore({ etag: () => { return Q(42); }, get: () => { return Q({ _metadata: { headers: { link: 'links' } }, elements: ['test'] }); } });
    const fetcher = createBaseFetcher({ requestor: requestor, store: store });
    return fetcher.fetch(request).then(request => {
      expect(request.document[0]).to.be.equal('test');
      expect(request.response.headers.link).to.be.equal('links');
      expect(request.response.statusCode).to.be.equal(304);
      expect(request.shouldSkip()).to.be.false;
      expect(request.contentOrigin).to.be.equal('cacheOfOrigin');
      expect(getArgs.options.headers['If-None-Match']).to.be.equal(42);
      expect(getArgs.url).to.be.equal(url);
    });
  });

  it('should skip for 304 without force', () => {
    const request = new Request('foo', 'http://test');
    const responses = [createResponse(null, 304, 42)];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const store = createBaseStore({ etag: () => { return Q(42); }, get: () => { return Q('test'); } });
    const fetcher = createBaseFetcher({ requestor: requestor, store: store });
    return fetcher.fetch(request).then(request => {
      expect(request.document).to.be.undefined;
      expect(request.shouldSkip()).to.be.true;
    });
  });

  it('should get from origin with originOnly fetch policy', () => {
    const request = new Request('foo', 'http://test');
    request.policy.fetch = 'originOnly';
    const responses = [createResponse('hey there')];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const fetcher = createBaseFetcher({ requestor: requestor });
    return fetcher.fetch(request).then(request => {
      expect(request.document).to.be.equal('hey there');
      expect(request.shouldSkip()).to.be.false;
    });
  });

  it('should pull from storage only storageOnly fetch policy', () => {
    const request = new Request('foo', 'http://test');
    request.policy.fetch = 'storageOnly';
    const store = createBaseStore({ get: () => { return Q({ _metadata: {}, id: 'test' }); } });
    const fetcher = createBaseFetcher({ store: store });
    return fetcher.fetch(request).then(request => {
      expect(request.document.id).to.be.equal('test');
      expect(request.shouldSkip()).to.be.false;
    });
  });

  it('should throw for bad codes', () => {
    const request = new Request('foo', 'http://test');
    const responses = [createResponse('test', 500)];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const store = createBaseStore({ etag: () => { return Q(null); } });
    const fetcher = createBaseFetcher({ requestor: requestor, store: store });
    return Q.try(() => {
      return fetcher.fetch(request);
    }).then(
      request => assert.fail(),
      error => expect(error.message.startsWith('Code 500')).to.be.true);
  });

  it('should throw for store etag errors', () => {
    const request = new Request('foo', 'http://test');
    const store = createBaseStore({ etag: () => { throw new Error('test'); } });
    const fetcher = createBaseFetcher({ store: store });
    return Q.try(() => {
      return fetcher.fetch(request);
    }).then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('test')
      );
  });

  it('should throw for requestor get errors', () => {
    const request = new Request('repos', 'http://test');
    const requestor = createBaseRequestor({ get: () => { throw new Error('test'); } });
    const store = createBaseStore({ etag: () => { return Q(42); } });
    const fetcher = createBaseFetcher({ requestor: requestor, store: store });
    return Q.try(() => {
      return fetcher.fetch(request);
    }).then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('test')
      );
  });

  it('should throw for store get errors', () => {
    const request = new Request('repos', 'http://test');
    request.policy = TraversalPolicy.update();
    const responses = [createResponse(null, 304, 42)];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const store = createBaseStore({ etag: () => { return Q(42); }, get: () => { throw new Error('test'); } });
    const fetcher = createBaseFetcher({ requestor: requestor, store: store });
    return Q.try(() => {
      return fetcher.fetch(request);
    }).then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('test')
      );
  });
});


function createResponse(body, code = 200, etag = null, remaining = 4000, reset = 0, headers = {}) {
  return {
    statusCode: code,
    headers: Object.assign({
      etag: etag,
      'x-ratelimit-remaining': remaining,
      'x-ratelimit-reset': reset ? reset : 0
    }, headers),
    body: body
  };
}

function createBaseStore({etag = null, upsert = null, get = null} = {}) {
  const result = {};
  result.etag = etag || (() => { assert.fail('should not etag'); });
  result.upsert = upsert || (() => { assert.fail('should not upsert'); });
  result.get = get || (() => assert.fail('should not get'));
  return result;
}

function createBaseFetcher({ requestor = createBaseRequestor(), store = createBaseStore(), tokenFactory = createBaseTokenFactory(), options = createBaseOptions() } = {}) {
  return new GitHubFetcher(requestor, store, tokenFactory, options.fetcher);
}

function createBaseRequestor({ get = null, getAll = null } = {}) {
  const result = {};
  result.get = get || (() => assert.fail('should not get'));
  result.getAll = getAll || (() => assert.fail('should not getAll'));
  return result;
}

function createBaseTokenFactory() {
  return { getToken: () => { return 'token'; } };
}

function createBaseOptions(logger = createBaseLog()) {
  return {
    fetcher: {
      logger: logger,
      tokenLowerBound: 50,
      forbiddenDelay: 120000
    }
  };
}

function createBaseLog({info = null, warn = null, error = null, verbose = null, silly = null} = {}) {
  const result = {};
  result.info = info || (() => { });
  result.warn = warn || (() => { });
  result.error = error || (() => { });
  result.verbose = verbose || ((message) => { console.log(message) });
  result.silly = silly || ((message) => { console.log(message) });
  result.level = 'silly';
  return result;
}

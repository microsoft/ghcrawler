const assert = require('chai').assert;
const chai = require('chai');
const Crawler = require('../lib/crawler');
const expect = require('chai').expect;
const extend = require('extend');
const Q = require('q');
const Request = require('../lib/request');

describe('Crawler fetch', () => {
  it('should skip skipped requests', () => {
    const request = new Request('foo', null);
    request.markSkip();
    const crawler = createBaseCrawler();
    crawler._fetch(request);
  });

  it('should skip requeued requests', () => {
    const request = new Request('foo', null);
    request.markRequeue();
    const crawler = createBaseCrawler();
    crawler._fetch(request);
  });

  it('should fetch one unseen document', () => {
    const request = new Request('foo', 'http://test');
    const responses = [createResponse('test')];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const store = createBaseStore({ etag: () => { return Q(null); } });
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return crawler._fetch(request).then(request => {
      expect(request.document).to.be.equal('test');
      expect(request.response.statusCode).to.be.equal(200);
      expect(request.shouldSkip()).to.be.false;
    });
  });

  it('should set subtype for collection requests', () => {
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
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return crawler._fetch(request).then(request => {
      expect(request.document).to.be.equal('test');
      expect(request.response.statusCode).to.be.equal(200);
      expect(request.shouldSkip()).to.be.false;
      expect(request.type).to.be.equal('collection');
      expect(request.subType).to.be.equal('repo');
      expect(etagArgs.type).to.be.equal('page');
      expect(etagArgs.url).to.be.equal(url);
      expect(getArgs.url).to.be.equal(url);
    });
  });

  it('should skip 409s', () => {
    const request = new Request('foo', 'http://test');
    const responses = [createResponse('test', 409)];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const store = createBaseStore({ etag: () => { return Q(null); } });
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return crawler._fetch(request).then(request => {
      expect(request.document).to.be.undefined;
      expect(request.shouldSkip()).to.be.true;
    });
  });

  it('should return cached content and not save and response for 304 with force', () => {
    const url = 'http://test';
    const request = new Request('repos', url);
    request.force = true;
    let getArgs = null;
    const responses = [createResponse(null, 304, 42)];
    const requestor = createBaseRequestor({
      get: (url, options) => { getArgs = { url: url, options: options }; return Q(responses.shift()); }
    });
    const store = createBaseStore({ etag: () => { return Q(42); }, get: () => { return Q('test'); } });
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return crawler._fetch(request).then(request => {
      expect(request.document).to.be.equal('test');
      expect(request.response.statusCode).to.be.equal(304);
      expect(request.shouldSkip()).to.be.false;
      expect(request.store).to.be.false;
      expect(getArgs.options.headers['If-None-Match']).to.be.equal(42);
      expect(getArgs.url).to.be.equal(url);
    });
  });

  it('should skip for 304 without force', () => {
    const request = new Request('foo', 'http://test');
    const responses = [createResponse(null, 304, 42)];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const store = createBaseStore({ etag: () => { return Q(42); }, get: () => { return Q('test'); } });
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return crawler._fetch(request).then(request => {
      expect(request.document).to.be.undefined;
      expect(request.response).to.be.undefined;
      expect(request.shouldSkip()).to.be.true;
    });
  });

  it('should throw for bad codes', () => {
    const request = new Request('foo', 'http://test');
    const responses = [createResponse('test', 500)];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const store = createBaseStore({ etag: () => { return Q(null); } });
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return Q().then(() => {
      return crawler._fetch(request);
    }).then(
      request => {
        assert.fail();
      },
      error => {
        expect(error.message.startsWith('Code: 500')).to.be.true;
      });
  });

  it('should throw for store etag errors', () => {
    const request = new Request('foo', 'http://test');
    const store = createBaseStore({ etag: () => { throw new Error('test'); } });
    const crawler = createBaseCrawler({ store: store });
    return Q().then(() => {
      return crawler._fetch(request);
    }).then(
      request => {
        assert.fail();
      },
      error => {
        expect(error.message).to.be.equal('test');
      });
  });

  it('should throw for requestor get errors', () => {
    const request = new Request('repos', 'http://test');
    const requestor = createBaseRequestor({
      get: () => { throw new Error('test'); }
    });
    const store = createBaseStore({ etag: () => { return Q(42); } });
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return Q().then(() => {
      return crawler._fetch(request);
    }).then(
      request => {
        assert.fail();
      },
      error => {
        expect(error.message).to.be.equal('test');
      });
  });

  it('should throw for store get errors', () => {
    const request = new Request('repos', 'http://test');
    request.force = true;
    const responses = [createResponse(null, 304, 42)];
    const requestor = createBaseRequestor({
      get: () => { return Q(responses.shift()); }
    });
    const store = createBaseStore({ etag: () => { return Q(42); }, get: () => { throw new Error('test'); } });
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return Q().then(() => {
      return crawler._fetch(request);
    }).then(
      request => {
        assert.fail();
      },
      error => {
        expect(error.message).to.be.equal('test');
      });
  });

});

function createResponse(body, code = 200, etag = null) {
  return {
    statusCode: code,
    headers: {
      etag: etag
    },
    body: body
  };
}

function create304Response(etag) {
  return {
    statusCode: 304,
    headers: {
      etag: etag
    }
  };
}

function createMultiPageResponse(target, body, previous, next, last, code = 200, error = null, remaining = 4000, reset = null) {
  return {
    headers: {
      'x-ratelimit-remaining': remaining,
      'x-ratelimit-reset': reset ? reset : 0,
      link: createLinkHeader(target, previous, next, last)
    },
    statusCode: code,
    body: body
  };
}

function createErrorResponse(error) {
  return {
    error: new Error(error)
  };
}

function createLinkHeader(target, previous, next, last) {
  separator = target.includes('?') ? '&' : '?';
  const firstLink = null; //`<${urlHost}/${target}${separator}page=1>; rel="first"`;
  const prevLink = previous ? `<${urlHost}/${target}${separator}page=${previous}>; rel="prev"` : null;
  const nextLink = next ? `<${urlHost}/${target}${separator}page=${next}>; rel="next"` : null;
  const lastLink = last ? `<${urlHost}/${target}${separator}page=${last}>; rel="last"` : null;
  return [firstLink, prevLink, nextLink, lastLink].filter(value => { return value !== null; }).join(',');
}

function createBaseCrawler({normalQueue = createBaseQueue(), priorityQueue = createBaseQueue(), deadLetterQueue = createBaseQueue(), store = createBaseStore(), locker = createBaseLocker, requestor = createBaseRequestor(), options = null, winston = createBaseLog() } = {}) {
  return new Crawler(normalQueue, priorityQueue, deadLetterQueue, store, locker, requestor, options, winston);
}

function createBaseQueue({ pop = null, push = null, done = null, abandon = null} = {}) {
  const result = {};
  result.pop = pop || (() => assert.fail('should not pop'));
  result.push = push || (() => assert.fail('should not push'));
  result.done = done || (() => assert.fail('should not done'));
  result.abandon = abandon || (() => assert.fail('should not abandon'));
  return result;
}

function createBaseStore({etag = null, upsert = null, get = null} = {}) {
  const result = {};
  result.etag = etag || (() => { assert.fail('should not etag'); });
  result.upsert = upsert || (() => { assert.fail('should not upsert'); });
  result.get = get || (() => assert.fail('should not get'));
  return result;
}

function createBaseLog({log = null, warn = null, error = null, verbose = null} = {}) {
  const result = {};
  result.log = log || (() => { });
  result.warn = warn || (() => { });
  result.error = error || (() => { });
  result.verbose = verbose || (() => { });
  return result;
}

function createBaseLocker({lock = null, unlock = null} = {}) {
  const result = {};
  result.lock = lock || (() => assert.fail('should not lock'));
  result.unlock = unlock || (() => assert.fail('should not unlock'));
  return result;
}

function createBaseRequestor({ get = null, getAll = null } = {}) {
  const result = {};
  result.get = get || (() => assert.fail('should not get'));
  result.getAll = getAll || (() => assert.fail('should not getAll'));
  return result;
}

const assert = require('chai').assert;
const chai = require('chai');
const Crawler = require('../lib/crawler');
const expect = require('chai').expect;
const extend = require('extend');
const Q = require('q');
const Request = require('../lib/request');

describe('Crawler get request', () => {
  it('should get from the priority queue first', () => {
    const priority = createBaseQueue({ pop: () => { return Q(new Request('priority', 'http://test')); } });
    const normal = createBaseQueue({ pop: () => { return Q(new Request('normal', 'http://test')); } });
    const locker = createBaseLocker({ lock: () => { return Q('locked'); } });
    const crawler = createBaseCrawler({ normal: normal, priority: priority, locker: locker });
    const requestBox = [];
    return crawler._getRequest(requestBox, 'test').then(request => {
      expect(request.type).to.be.equal('priority');
      expect(request.originQueue === priority).to.be.true;
      expect(request.lock).to.be.equal('locked');
      expect(request.crawlerName).to.be.equal('test');
      expect(request).to.be.equal(requestBox[0]);
    });
  });

  it('should get from the normal queue if no priority', () => {
    const priority = createBaseQueue({ pop: () => { return Q(null); } });
    const normal = createBaseQueue({ pop: () => { return Q(new Request('normal', 'http://test')); } });
    const locker = createBaseLocker({ lock: () => { return Q('locked'); } });
    const crawler = createBaseCrawler({ normal: normal, priority: priority, locker: locker });
    const requestBox = [];
    return crawler._getRequest(requestBox, 'test').then(request => {
      expect(request.type).to.be.equal('normal');
      expect(request.originQueue === normal).to.be.true;
      expect(request.lock).to.be.equal('locked');
      expect(request.crawlerName).to.be.equal('test');
      expect(request).to.be.equal(requestBox[0]);
    });
  });

  it('should return a dummy skip/delay request if none are queued', () => {
    const priority = createBaseQueue({ pop: () => { return Q(null); } });
    const normal = createBaseQueue({ pop: () => { return Q(null); } });
    const crawler = createBaseCrawler({ normal: normal, priority: priority });
    const requestBox = [];
    return crawler._getRequest(requestBox, 'test').then(request => {
      expect(request.type).to.be.equal('wait');
      expect(request.lock).to.be.undefined;
      expect(request.shouldSkip()).to.be.true;
      expect(request.flowControl).to.be.equal('delay');
      expect(request.crawlerName).to.be.equal('test');
      expect(request).to.be.equal(requestBox[0]);
    });
  });

  it('should throw when normal pop errors', () => {
    const priority = createBaseQueue({ pop: () => { return Q(null); } });
    const normal = createBaseQueue({ pop: () => { throw new Error('normal test'); } });
    const crawler = createBaseCrawler({ normal: normal, priority: priority });
    const requestBox = [];
    return crawler._getRequest(requestBox, 'test').then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('normal test')
    );
  });

  it('should throw when priority pop errors', () => {
    const priority = createBaseQueue({ pop: () => { throw new Error('priority test'); } });
    const normal = createBaseQueue({ pop: () => { return Q(null); } });
    const crawler = createBaseCrawler({ normal: normal, priority: priority });
    const requestBox = [];
    return crawler._getRequest(requestBox, 'test').then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('priority test')
    );
  });

  it('should throw when acquire lock errors', () => {
    const priority = createBaseQueue({ pop: () => { return Q(new Request('priority', 'http://test')); } });
    const normal = createBaseQueue({ pop: () => { return Q(null); } });
    const locker = createBaseLocker({ lock: () => { throw new Error('locker error'); } });
    const crawler = createBaseCrawler({ normal: normal, priority: priority, locker: locker });
    const requestBox = [];
    return crawler._getRequest(requestBox, 'test').then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('locker error')
    );
  });

  it('should abandon the request when the lock cannot be acquired', () => {
    const abandoned = [];
    const priority = createBaseQueue({
      pop: () => { return Q(new Request('priority', 'http://test')); },
      abandon: request => {
        abandoned.push(request);
        return Q();
      }
    });
    const normal = createBaseQueue({ pop: () => { return Q(null); } });
    const locker = createBaseLocker({ lock: () => { return Q.reject(new Error('locker error')); } });
    const crawler = createBaseCrawler({ normal: normal, priority: priority, locker: locker });
    const requestBox = [];
    return crawler._getRequest(requestBox, 'test').then(
      request => assert.fail(),
      error => {
        expect(error.message).to.be.equal('locker error');
        expect(abandoned.length).to.be.equal(1);
      });
  });

  it('should get lock error even if abandon fails', () => {
    const abandoned = [];
    const priority = createBaseQueue({
      pop: () => { return Q(new Request('priority', 'http://test')); },
      abandon: request => { throw new Error('Abandon error'); }
    });
    const normal = createBaseQueue({ pop: () => { return Q(null); } });
    const locker = createBaseLocker({ lock: () => { return Q.reject(new Error('locker error')); } });
    const crawler = createBaseCrawler({ normal: normal, priority: priority, locker: locker });
    const requestBox = [];
    return crawler._getRequest(requestBox, 'test').then(
      request => assert.fail(),
      error => {
        expect(error.message).to.be.equal('locker error');
        expect(abandoned.length).to.be.equal(0);
      });
  });

});

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
    return Q.try(() => {
      return crawler._fetch(request);
    }).then(
      request => assert.fail(),
      error => expect(error.message.startsWith('Code: 500')).to.be.true
      );
  });

  it('should throw for store etag errors', () => {
    const request = new Request('foo', 'http://test');
    const store = createBaseStore({ etag: () => { throw new Error('test'); } });
    const crawler = createBaseCrawler({ store: store });
    return Q.try(() => {
      return crawler._fetch(request);
    }).then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('test')
      );
  });

  it('should throw for requestor get errors', () => {
    const request = new Request('repos', 'http://test');
    const requestor = createBaseRequestor({
      get: () => { throw new Error('test'); }
    });
    const store = createBaseStore({ etag: () => { return Q(42); } });
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return Q.try(() => {
      return crawler._fetch(request);
    }).then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('test')
      );
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
    return Q.try(() => {
      return crawler._fetch(request);
    }).then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('test')
      );
  });
});

describe('Crawler filtering', () => {
  it('should filter', () => {
    const config = { orgFilter: new Set(['microsoft']) };
    const crawler = createBaseCrawler({ options: config });
    expect(crawler._shouldInclude('repo', 'http://api.github.com/repo/microsoft/test')).to.be.true;
    expect(crawler._shouldInclude('repos', 'http://api.github.com/repos/microsoft/test')).to.be.true;
    expect(crawler._shouldInclude('org', 'http://api.github.com/org/microsoft/test')).to.be.true;

    expect(crawler._shouldInclude('repo', 'http://api.github.com/repo/test/test')).to.be.false;
    expect(crawler._shouldInclude('repos', 'http://api.github.com/repos/test/test')).to.be.false;
    expect(crawler._shouldInclude('org', 'http://api.github.com/org/test/test')).to.be.false;
  });

  it('should not filter if no config', () => {
    const config = {};
    const crawler = createBaseCrawler({ options: config });
    expect(crawler._shouldInclude('repo', 'http://api.github.com/repo/microsoft/test')).to.be.true;
    expect(crawler._shouldInclude('repo', 'http://api.github.com/repo/test/test')).to.be.true;
  });
});

describe('Crawler queue', () => {
  it('should not queue if filtered', () => {
    const config = { orgFilter: new Set(['test']) };
    const queue = [];
    const normal = createBaseQueue({ push: (request) => { queue.push(request); return Q(); } });
    const newRequest = new Request('repo', 'http://api.github.com/repo/microsoft/test');
    request = { promises: [] };
    const crawler = createBaseCrawler({ normal: normal, options: config });
    crawler.queue(request, newRequest);
    expect(request.promises.length).to.be.equal(0);
    expect(queue.length).to.be.equal(0);
  });

  it('should queue if not filtered', () => {
    const config = { orgFilter: new Set(['microsoft']) };
    const queue = [];
    const normal = createBaseQueue({ push: (request) => { queue.push(request); return Q(); } });
    const newRequest = new Request('repo', 'http://api.github.com/repo/microsoft/test');
    request = { promises: [] };
    const crawler = createBaseCrawler({ normal: normal, options: config });
    crawler.queue(request, newRequest);
    expect(request.promises.length).to.be.equal(1);
    expect(queue.length).to.be.equal(1);
    expect(queue[0] !== newRequest).to.be.true;
    expect(queue[0].type === newRequest.type).to.be.true;
    expect(queue[0].url === newRequest.url).to.be.true;
  });

  it('should queue in supplied queue', () => {
    const config = { orgFilter: new Set(['microsoft']) };
    const queue = [];
    const supplied = createBaseQueue({ push: (request) => { queue.push(request); return Q(); } });
    const newRequest = new Request('repo', 'http://api.github.com/repo/microsoft/test');
    request = { promises: [] };
    const crawler = createBaseCrawler({ options: config });
    crawler.queue(request, newRequest, supplied);
    expect(request.promises.length).to.be.equal(1);
    expect(queue.length).to.be.equal(1);
    expect(queue[0] !== newRequest).to.be.true;
    expect(queue[0].type === newRequest.type).to.be.true;
    expect(queue[0].url === newRequest.url).to.be.true;
  });
});

describe('Crawler requeue', () => {
  it('should return if queuing not needed', () => {
    const request = new Request('test', null);
    const crawler = createBaseCrawler();
    // The crawler will throw if it tries to do anything
    crawler._requeue(request);
  });

  it('should requeue in same queue as before', () => {
    const queue = [];
    const normal = createBaseQueue({ push: (request) => { queue.push(request); return Q(); } });
    const crawler = createBaseCrawler({ normal: normal });
    for (let i = 0; i < 5; i++) {
      const request = new Request('test', 'http://api.github.com/repo/microsoft/test');
      request.markRequeue();
      request.promises = [];
      request.originQueue = normal;
      request.attemptCount = i === 0 ? null : i;
      crawler._requeue(request);
      expect(request.promises.length).to.be.equal(1);
      expect(queue.length).to.be.equal(1);
      expect(queue[0] !== request).to.be.true;
      expect(queue[0].type === request.type).to.be.true;
      expect(queue[0].url === request.url).to.be.true;
      expect(queue[0].attemptCount).to.be.equal(i + 1);
      // pop the request to get ready for the next iteration
      queue.shift();
    }
  });

  it('should requeue in deadletter queue after 5 attempts', () => {
    const queue = [];
    const deadLetterQueue = [];
    const normal = createBaseQueue({ push: (request) => { queue.push(request); return Q(); } });
    const deadLetter = createBaseQueue({ push: (request) => { deadLetterQueue.push(request); return Q(); } });
    const request = new Request('test', 'http://api.github.com/repo/microsoft/test');
    request.promises = [];
    request.attemptCount = 5;
    request.markRequeue();
    request.originQueue = normal;
    const crawler = createBaseCrawler({ normal: normal, deadLetter: deadLetter });
    crawler._requeue(request);
    expect(request.promises.length).to.be.equal(1);
    expect(queue.length).to.be.equal(0);
    expect(deadLetterQueue.length).to.be.equal(1);
    expect(deadLetterQueue[0] !== request).to.be.true;
    expect(deadLetterQueue[0].type === request.type).to.be.true;
    expect(deadLetterQueue[0].url === request.url).to.be.true;
    expect(deadLetterQueue[0].attemptCount).to.be.equal(6);
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

function createBaseCrawler({normal = createBaseQueue(), priority = createBaseQueue(), deadLetter = createBaseQueue(), store = createBaseStore(), locker = createBaseLocker, requestor = createBaseRequestor(), options = {}, winston = createBaseLog() } = {}) {
  return new Crawler(normal, priority, deadLetter, store, locker, requestor, options, winston);
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

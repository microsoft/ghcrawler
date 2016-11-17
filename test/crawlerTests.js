const assert = require('chai').assert;
const chai = require('chai');
const Crawler = require('../lib/crawler');
const expect = require('chai').expect;
const extend = require('extend');
const Q = require('q');
const QueueSet = require('../lib/queueSet');
const Request = require('../lib/request');
const sinon = require('sinon');

describe('Crawler get request', () => {
  it('should get from the priority queue first', () => {
    const priority = createBaseQueue({ pop: () => { return Q(new Request('priority', 'http://test')); } });
    const normal = createBaseQueue({ pop: () => { return Q(new Request('normal', 'http://test')); } });
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const locker = createBaseLocker({ lock: () => { return Q('locked'); } });
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    const requestBox = [];
    return crawler._getRequest(requestBox, 'test').then(request => {
      expect(request.type).to.be.equal('priority');
      expect(request._originQueue === queues.priority).to.be.true;
      expect(request.lock).to.be.equal('locked');
      expect(request.crawlerName).to.be.equal('test');
      expect(request).to.be.equal(requestBox[0]);
    });
  });

  it('should get from the normal queue if no priority', () => {
    const priority = createBaseQueue({ pop: () => { return Q(null); } });
    const normal = createBaseQueue({ pop: () => { return Q(new Request('normal', 'http://test')); } });
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const locker = createBaseLocker({ lock: () => { return Q('locked'); } });
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    const requestBox = [];
    return crawler._getRequest(requestBox, 'test').then(request => {
      expect(request.type).to.be.equal('normal');
      expect(request._originQueue === queues.normal).to.be.true;
      expect(request.lock).to.be.equal('locked');
      expect(request.crawlerName).to.be.equal('test');
      expect(request).to.be.equal(requestBox[0]);
    });
  });

  it('should return a dummy skip/delay request if none are queued', () => {
    const priority = createBaseQueue({ pop: () => { return Q(null); } });
    const normal = createBaseQueue({ pop: () => { return Q(null); } });
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const crawler = createBaseCrawler({ queues: queues });
    const requestBox = [];
    return crawler._getRequest(requestBox, 'test').then(request => {
      expect(request.type).to.be.equal('_blank');
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
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const crawler = createBaseCrawler({ queues: queues });
    const requestBox = [];
    return crawler._getRequest(requestBox, 'test').then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('normal test')
    );
  });

  it('should throw when priority pop errors', () => {
    const priority = createBaseQueue({ pop: () => { throw new Error('priority test'); } });
    const normal = createBaseQueue({ pop: () => { return Q(null); } });
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const crawler = createBaseCrawler({ queues: queues });
    const requestBox = [];
    return crawler._getRequest(requestBox, 'test').then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('priority test')
    );
  });

  it('should throw when acquire lock errors', () => {
    const priority = createBaseQueue({ pop: () => { return Q(new Request('priority', 'http://test')); } });
    const normal = createBaseQueue({ pop: () => { return Q(null); } });
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const locker = createBaseLocker({ lock: () => { throw new Error('locker error'); } });
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
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
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const locker = createBaseLocker({ lock: () => { return Q.reject(new Error('locker error')); } });
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
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
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const locker = createBaseLocker({ lock: () => { return Q.reject(new Error('locker error')); } });
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
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
    return crawler._fetch(request);
  });

  it('should skip requeued requests', () => {
    const request = new Request('foo', null);
    request.markRequeue();
    const crawler = createBaseCrawler();
    return crawler._fetch(request);
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

  it('should requeue and delay on 403 forbidden throttling', () => {
    const request = new Request('foo', 'http://test');
    const responses = [createResponse('test', 403)];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const store = createBaseStore({ etag: () => { return Q(null); } });
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return crawler._fetch(request).then(request => {
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
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return crawler._fetch(request).then(request => {
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
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return crawler._fetch(request).then(request => {
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
    const requestor = createBaseRequestor({ get: () => { throw new Error('test'); } });
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
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
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
    expect(crawler._filter(new Request('repo', 'http://api.github.com/repo/microsoft/test')).shouldSkip()).to.be.false;
    expect(crawler._filter(new Request('repos', 'http://api.github.com/repos/microsoft/test')).shouldSkip()).to.be.false;
    expect(crawler._filter(new Request('org', 'http://api.github.com/org/microsoft/test')).shouldSkip()).to.be.false;

    expect(crawler._filter(new Request('repo', 'http://api.github.com/repo/test/test')).shouldSkip()).to.be.true;
    expect(crawler._filter(new Request('repos', 'http://api.github.com/repos/test/test')).shouldSkip()).to.be.true;
    expect(crawler._filter(new Request('org', 'http://api.github.com/org/test/test')).shouldSkip()).to.be.true;

    expect(crawler._filter(new Request('foo', 'http://api.github.com/org/test/test')).shouldSkip()).to.be.false;
  });

  it('should not filter if no config', () => {
    const config = {};
    const crawler = createBaseCrawler({ options: config });
    expect(crawler._filter(new Request('repo', 'http://api.github.com/repo/microsoft/test')).shouldSkip()).to.be.false;
    expect(crawler._filter(new Request('repo', 'http://api.github.com/repo/test/test')).shouldSkip()).to.be.false;
    expect(crawler._filter(new Request('foo', 'http://api.github.com/repo/test/test')).shouldSkip()).to.be.false;
  });
});

describe('Crawler error handler', () => {
  it('should mark for requeuing if there is a request', () => {
    const box = [];
    box.push(new Request('repo', 'http://test.com'));
    const crawler = createBaseCrawler();
    const error = 'error';
    const request = crawler._errorHandler(box, error);
    expect(request.shouldSkip()).to.be.true;
    expect(request.shouldRequeue()).to.be.true;
    expect(request.outcome).to.be.equal('Error');
    expect(request.message).to.be.equal(error);
  });

  it('should bail and delay if no request', () => {
    const box = [];
    const crawler = createBaseCrawler();
    const error = 'error';
    const request = crawler._errorHandler(box, error);
    expect(request.message).to.be.equal(error);
    expect(request.flowControl).to.be.equal('delay');
  });
});

describe('Crawler log outcome', () => {
  it('should log the Processed case', () => {
    const info = [];
    const error = [];
    const logger = createBaseLog({
      info: value => info.push(value),
      error: value => error.push(value)
    });
    const newRequest = new Request('repo', 'http://api.github.com/repo/microsoft/test');
    const crawler = createBaseCrawler({ logger: logger });
    crawler._logOutcome(newRequest);
    expect(info.length).to.be.equal(1);
    expect(info[0].includes('Processed')).to.be.true;
    expect(error.length).to.be.equal(0);
  });

  it('should log explicit outcomes', () => {
    const info = [];
    const error = [];
    const logger = createBaseLog({
      info: value => info.push(value),
      error: value => error.push(value)
    });
    const newRequest = new Request('repo', 'http://api.github.com/repo/microsoft/test');
    newRequest.markSkip('test', 'message');
    const crawler = createBaseCrawler({ logger: logger });
    crawler._logOutcome(newRequest);
    expect(info.length).to.be.equal(1);
    expect(info[0].includes('test')).to.be.true;
    expect(info[0].includes('message')).to.be.true;
    expect(error.length).to.be.equal(0);
  });

  it('should log errors', () => {
    const info = [];
    const error = [];
    const logger = createBaseLog({
      info: value => info.push(value),
      error: value => error.push(value)
    });
    const newRequest = new Request('repo', 'http://api.github.com/repo/microsoft/test');
    newRequest.markSkip('Error', 'message');
    const crawler = createBaseCrawler({ logger: logger });
    crawler._logOutcome(newRequest);
    expect(error.length).to.be.equal(1);
    expect(error[0] instanceof Error).to.be.true;
    expect(error[0].message).to.be.equal('message');
    expect(info.length).to.be.equal(0);
  });


  it('should log errors cases with Error objects', () => {
    const info = [];
    const error = [];
    const logger = createBaseLog({
      info: value => info.push(value),
      error: value => error.push(value)
    });
    const newRequest = new Request('repo', 'http://api.github.com/repo/microsoft/test');
    newRequest.markSkip('Error', new Error('message'));
    const crawler = createBaseCrawler({ logger: logger });
    crawler._logOutcome(newRequest);
    expect(error.length).to.be.equal(1);
    expect(error[0] instanceof Error).to.be.true;
    expect(error[0].message).to.be.equal('message');
    expect(info.length).to.be.equal(0);
  });
});

describe('Crawler queue', () => {
  it('should not queue if filtered', () => {
    const config = { orgFilter: new Set(['test']) };
    const queue = [];
    const normal = createBaseQueue({ push: request => { queue.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal });
    const request = new Request('repo', 'http://api.github.com/repo/microsoft/test');
    const crawler = createBaseCrawler({ queues: queues, options: config });
    crawler.queue(request);
    expect(request.promises.length).to.be.equal(0);
    expect(queue.length).to.be.equal(0);
  });

  it('should queue if not filtered', () => {
    const config = { orgFilter: new Set(['microsoft']) };
    const queue = [];
    const normal = createBaseQueue({ push: request => { queue.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal });
    const request = new Request('repo', 'http://api.github.com/repo/microsoft/test');
    const crawler = createBaseCrawler({ queues: queues, options: config });
    request.track(crawler.queue(request));
    expect(request.promises.length).to.be.equal(1);
    expect(queue.length).to.be.equal(1);
    expect(queue[0] !== request).to.be.true;
    expect(queue[0].type === request.type).to.be.true;
    expect(queue[0].url === request.url).to.be.true;
  });

  // TODO
  it('should queue in supplied queue', () => {
    const config = { orgFilter: new Set(['microsoft']) };
    const queue = [];
    const normal = createBaseQueue({ push: request => { queue.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal });
    const request = new Request('repo', 'http://api.github.com/repo/microsoft/test');
    const crawler = createBaseCrawler({ queues: queues, options: config });
    request.track(crawler.queue(request));
    expect(request.promises.length).to.be.equal(1);
    expect(queue.length).to.be.equal(1);
    expect(queue[0] !== request).to.be.true;
    expect(queue[0].type === request.type).to.be.true;
    expect(queue[0].url === request.url).to.be.true;
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
    const normal = createBaseQueue({ push: request => { queue.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal });
    const crawler = createBaseCrawler({ queues: queues });
    for (let i = 0; i < 5; i++) {
      const request = new Request('test', 'http://api.github.com/repo/microsoft/test');
      request.markRequeue();
      request._originQueue = normal;
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
    const deadletterQueue = [];
    const normal = createBaseQueue({ push: request => { queue.push(request); return Q(); } });
    const deadletter = createBaseQueue({ push: request => { deadletterQueue.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal, deadletter: deadletter });
    const request = new Request('test', 'http://api.github.com/repo/microsoft/test');
    request.attemptCount = 5;
    request.markRequeue();
    request._originQueue = normal;
    const crawler = createBaseCrawler({ queues: queues });
    crawler._requeue(request);
    expect(request.promises.length).to.be.equal(1);
    expect(queue.length).to.be.equal(0);
    expect(deadletterQueue.length).to.be.equal(1);
    expect(deadletterQueue[0] !== request).to.be.true;
    expect(deadletterQueue[0].type === request.type).to.be.true;
    expect(deadletterQueue[0].url === request.url).to.be.true;
    expect(deadletterQueue[0].attemptCount).to.be.equal(6);
  });
});

describe('Crawler complete request', () => {
  it('should unlock, dequeue and return the request being completed', () => {
    const done = [];
    const unlock = [];
    const normal = createBaseQueue({ done: request => { done.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal });
    const locker = createBaseLocker({ unlock: request => { unlock.push(request); return Q(); } });
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.lock = 42;
    originalRequest._originQueue = normal;
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    return crawler._completeRequest(originalRequest).then(request => {
      expect(request === originalRequest).to.be.true;
      expect(request.lock).to.be.null;
      expect(done.length).to.be.equal(1);
      expect(done[0] === request).to.be.true;
      expect(unlock.length).to.be.equal(1);
      expect(unlock[0]).to.be.equal(42);
    });
  });

  it('should requeue the request being completed if needed', () => {
    const queue = [];
    const done = [];
    const unlock = [];
    const normal = createBaseQueue({
      push: request => { queue.push(request); return Q(); },
      done: request => { done.push(request); return Q(); }
    });
    const queues = createBaseQueues({ normal: normal });
    const locker = createBaseLocker({ unlock: request => { unlock.push(request); return Q(); } });
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.markRequeue();
    originalRequest.lock = 42;
    originalRequest._originQueue = normal;
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    return crawler._completeRequest(originalRequest).then(request => {
      expect(request === originalRequest).to.be.true;
      expect(request.lock).to.be.null;
      expect(queue.length).to.be.equal(1);
      expect(queue[0] !== request).to.be.true;
      expect(queue[0].type).to.be.equal(originalRequest.type);
      expect(queue[0].url).to.be.equal(originalRequest.url);
      expect(done.length).to.be.equal(1);
      expect(done[0] === request).to.be.true;
      expect(unlock.length).to.be.equal(1);
      expect(unlock[0]).to.be.equal(42);
    });
  });


  it('should do all right things for requests with no url', () => {
    const done = [];
    const normal = createBaseQueue({ done: request => { done.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal });
    const originalRequest = new Request('test', null);
    originalRequest.markRequeue();
    originalRequest.lock = 42;
    originalRequest._originQueue = normal;
    const crawler = createBaseCrawler({ queues: queues });
    return crawler._completeRequest(originalRequest).then(request => {
      expect(request === originalRequest).to.be.true;
      expect(done.length).to.be.equal(1);
      expect(done[0] === request).to.be.true;
    });
  });

  it('should wait for all promises to complete', () => {
    const done = [];
    const unlock = [];
    const promiseValue = [];
    const normal = createBaseQueue({
      done: request => {
        if (!promiseValue[0]) assert.fail();
        done.push(request);
        return Q();
      }
    });
    const queues = createBaseQueues({ normal: normal });
    const locker = createBaseLocker({
      unlock: request => {
        if (!promiseValue[0]) assert.fail();
        unlock.push(request);
        return Q();
      }
    });
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.lock = 42;
    originalRequest._originQueue = normal;
    originalRequest.promises = [Q.delay(1).then(() => promiseValue[0] = 13)];
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    return crawler._completeRequest(originalRequest).then(
      request => {
        expect(request === originalRequest).to.be.true;
        expect(request.lock).to.be.null;
        expect(done.length).to.be.equal(1);
        expect(done[0] === request).to.be.true;
        expect(unlock.length).to.be.equal(1);
        expect(unlock[0]).to.be.equal(42);
        expect(promiseValue[0]).to.be.equal(13);
      },
      error => assert.fail());
  });

  it('still dequeues and unlocks if promises fail', () => {
    const done = [];
    const unlock = [];
    const normal = createBaseQueue({ done: request => { done.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal });
    const locker = createBaseLocker({ unlock: request => { unlock.push(request); return Q(); } });
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.lock = 42;
    originalRequest._originQueue = normal;
    originalRequest.promises = [Q.reject(13)];
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    return crawler._completeRequest(originalRequest).then(
      request => assert.fail(),
      error => {
        expect(done.length).to.be.equal(1);
        expect(done[0] === originalRequest).to.be.true;
        expect(unlock.length).to.be.equal(1);
        expect(unlock[0]).to.be.equal(42);
      });
  });

  it('still dequeues when unlocking fails', () => {
    const done = [];
    const unlock = [];
    const normal = createBaseQueue({ done: request => { done.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal });
    const locker = createBaseLocker({ unlock: () => { throw new Error('sigh'); } });
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.lock = 42;
    originalRequest._originQueue = normal;
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    return crawler._completeRequest(originalRequest).then(
      request => {
        expect(request === originalRequest).to.be.true;
        expect(request.lock).to.be.null;
        expect(done.length).to.be.equal(1);
        expect(done[0] === request).to.be.true;
        expect(unlock.length).to.be.equal(0);
      },
      error => assert.fail());
  });

  it('still unlocks when dequeue fails', () => {
    const done = [];
    const unlock = [];
    const normal = createBaseQueue({ done: () => { throw new Error('sigh'); } });
    const queues = createBaseQueues({ normal: normal });
    const locker = createBaseLocker({ unlock: request => { unlock.push(request); return Q(); } });
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.lock = 42;
    originalRequest._originQueue = normal;
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    return crawler._completeRequest(originalRequest).then(
      request => assert.fail(),
      error => {
        expect(done.length).to.be.equal(0);
        expect(unlock.length).to.be.equal(1);
        expect(unlock[0]).to.be.equal(42);
      });
  });
});

describe('Crawler convert to document', () => {
  it('should skip if skipping', () => {
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.markSkip();
    originalRequest.document = {};
    const crawler = createBaseCrawler();
    return crawler._convertToDocument(originalRequest).then(request => {
      expect(request === originalRequest).to.be.true;
      expect(Object.keys(request.document).length).to.be.equal(0);
    });
  });

  it('should configure the document and metadata', () => {
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.response = {
      headers: { etag: 42 }
    };
    originalRequest.document = {};
    const crawler = createBaseCrawler();
    return crawler._convertToDocument(originalRequest).then(request => {
      expect(request === originalRequest).to.be.true;
      const metadata = request.document._metadata;
      expect(metadata.url).to.be.equal(request.url);
      expect(metadata.type).to.be.equal(request.type);
      expect(metadata.etag).to.be.equal(42);
      expect(metadata.links).to.be.not.null;
      expect(metadata.fetchedAt).to.be.not.null;
    });
  });

  it('should wrap array documents in an object', () => {
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.response = {
      headers: { etag: 42 }
    };
    const array = [1, 2, 3];
    originalRequest.document = array;
    const crawler = createBaseCrawler();
    return crawler._convertToDocument(originalRequest).then(request => {
      expect(request === originalRequest).to.be.true;
      const metadata = request.document._metadata;
      expect(metadata.url).to.be.equal(request.url);
      expect(metadata.type).to.be.equal(request.type);
      expect(metadata.etag).to.be.equal(42);
      expect(metadata.links).to.be.not.null;
      expect(metadata.fetchedAt).to.be.not.null;
      expect(request.document.elements === array).to.be.true;
    });
  });
});

describe('Crawler process document', () => {
  it('should skip if skipping', () => {
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.markSkip();
    const crawler = createBaseCrawler();
    return crawler._processDocument(originalRequest).then(request => {
      expect(request === originalRequest).to.be.true;
    });
  });

  it('should invoke a handler', () => {
    const originalRequest = new Request('test', 'http://test.com');
    const doc = { _metadata: {} };
    originalRequest.document = doc;
    const crawler = createBaseCrawler();
    const processorBox = [];
    crawler.processor.test = request => {
      processorBox[0] = 42;
      request.document.cool = 'content';
      return request.document;
    };
    return crawler._processDocument(originalRequest).then(request => {
      expect(request === originalRequest).to.be.true;
      expect(processorBox.length).to.be.equal(1);
      expect(processorBox[0]).to.be.equal(42);
      expect(request.document === doc).to.be.true;
      expect(request.document.cool).to.be.equal('content');
    });
  });

  it('should skip if no handler is found', () => {
    const originalRequest = new Request('test', 'http://test.com');
    const doc = { _metadata: {} };
    originalRequest.document = doc;
    const crawler = createBaseCrawler();
    return crawler._processDocument(originalRequest).then(request => {
      expect(request === originalRequest).to.be.true;
      expect(request.shouldSkip()).to.be.true;
    });
  });

  it('should throw if the handler throws', () => {
    const originalRequest = new Request('test', 'http://test.com');
    const doc = { _metadata: {} };
    originalRequest.document = doc;
    const crawler = createBaseCrawler();
    crawler.processor.test = request => { throw new Error('bummer'); };
    return Q.try(() => {
      crawler._processDocument(originalRequest)
    }).then(
      request => assert.fail(),
      error => { expect(error.message).to.be.equal('bummer'); });
  });
});

describe('Crawler store document', () => {
  it('should skip if skipping', () => {
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.markSkip();
    const crawler = createBaseCrawler();
    return crawler._storeDocument(originalRequest).then(request => {
      expect(request === originalRequest).to.be.true;
    });
  });

  it('should actually store', () => {
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.document = { something: 'interesting' };
    const storeBox = [];
    const store = createBaseStore({ upsert: document => { storeBox[0] = document; return Q('token'); } });
    const crawler = createBaseCrawler({ store: store });
    return crawler._storeDocument(originalRequest).then(request => {
      expect(request === originalRequest).to.be.true;
      expect(request.upsert).to.be.equal('token');
      expect(storeBox.length).to.be.equal(1);
      expect(storeBox[0].something).to.be.equal('interesting');
    });
  });

  it('should throw if the store throws', () => {
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.document = { something: 'interesting' };
    const storeBox = [];
    const store = createBaseStore({ upsert: () => { throw new Error('problem'); } });
    const crawler = createBaseCrawler({ store: store });
    return Q.try(() => {
      crawler._storeDocument(originalRequest)
    }).then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('problem'));
  });
});

describe('Crawler whole meal deal', () => {
  it('should delay starting next iteration when markDelay', () => {
    const crawler = createBaseCrawler();
    sinon.stub(crawler, 'start', () => Q());
    const clock = sinon.useFakeTimers();
    sinon.spy(clock, 'setTimeout');

    const request = new Request('user', 'http://test.com/users/user1');
    request.markDelay();

    crawler._startNext('test', request);
    expect(clock.setTimeout.getCall(0).args[1]).to.be.equal(1000);
  });

  it('should delay starting next iteration when delayUntil', () => {
    const crawler = createBaseCrawler();
    sinon.stub(crawler, 'start', () => Q());
    const clock = sinon.useFakeTimers();
    sinon.spy(clock, 'setTimeout');

    const request = new Request('user', 'http://test.com/users/user1');
    request.delayUntil(323);

    crawler._startNext('test', request);
    expect(clock.setTimeout.getCall(0).args[1]).to.be.equal(323);
  });

  it('should delay starting next iteration when delayFor', () => {
    const crawler = createBaseCrawler();
    sinon.stub(crawler, 'start', () => Q());
    const clock = sinon.useFakeTimers();
    sinon.spy(clock, 'setTimeout');

    const request = new Request('user', 'http://test.com/users/user1');
    request.delayFor(451);

    crawler._startNext('test', request);
    expect(clock.setTimeout.getCall(0).args[1]).to.be.equal(451);
  });

  it('should process normal requests', () => {
    const crawler = createFullCrawler();
    sinon.stub(crawler, '_startNext', () => Q());

    crawler.queues.normal.requests = [new Request('user', 'http://test.com/users/user1')];
    crawler.requestor.responses = [createResponse({ id: 42, repos_url: 'http://test.com/users/user1/repos' })];
    return Q.try(() => {
      return crawler.start('test');
    }).then(() => {
      expect(crawler.queues.priority.pop.callCount).to.be.equal(1, 'priority call count');
      expect(crawler.queues.normal.pop.callCount).to.be.equal(1, 'normal call count');

      const lock = crawler.locker.lock;
      expect(lock.callCount).to.be.equal(1, 'lock call count');
      expect(lock.getCall(0).args[0]).to.be.equal('http://test.com/users/user1');

      const etag = crawler.store.etag;
      expect(etag.callCount).to.be.equal(1);
      expect(etag.getCall(0).args[0]).to.be.equal('user');
      expect(etag.getCall(0).args[1]).to.be.equal('http://test.com/users/user1');

      const requestorGet = crawler.requestor.get;
      expect(requestorGet.callCount).to.be.equal(1);
      expect(requestorGet.getCall(0).args[0]).to.be.equal('http://test.com/users/user1');

      const process = crawler.processor.process;
      expect(process.callCount).to.be.equal(1);
      expect(process.getCall(0).args[0].type).to.be.equal('user');

      const upsert = crawler.store.upsert;
      expect(upsert.callCount).to.be.equal(1);
      const document = upsert.getCall(0).args[0];
      expect(document.id).to.be.equal(42);
      expect(document._metadata.url).to.be.equal('http://test.com/users/user1');

      const unlock = crawler.locker.unlock;
      expect(unlock.callCount).to.be.equal(1);
      expect(unlock.getCall(0).args[0]).to.be.equal('lockToken');

      expect(crawler.queues.normal.done.callCount).to.be.equal(1);

      expect(crawler.logger.error.callCount).to.be.equal(1);
    });
  });

  it('should empty request queues', () => {
    // TODO
  });

  it('should handle getRequest reject', () => {
    const crawler = createFullCrawler();
    sinon.stub(crawler, '_startNext', () => Q());

    // setup a problem popping
    const normal = createBaseQueue();
    sinon.stub(normal, 'pop', () => { throw Error('cant pop') });
    sinon.stub(normal, 'push', request => { return Q(); });
    sinon.spy(normal, 'done');
    crawler.queues.normal = normal;

    crawler.requestor.responses = [createResponse(null, 500)];
    return Q.try(() => {
      return crawler.start('test');
    }).then(() => {
      expect(crawler.queues.priority.pop.callCount).to.be.equal(1);
      expect(crawler.queues.normal.pop.callCount).to.be.equal(1);

      const lock = crawler.locker.lock;
      expect(lock.callCount).to.be.equal(0);

      const etag = crawler.store.etag;
      expect(etag.callCount).to.be.equal(0);

      const requestorGet = crawler.requestor.get;
      expect(requestorGet.callCount).to.be.equal(0);

      const push = crawler.queues.normal.push;
      expect(push.callCount).to.be.equal(0);

      const upsert = crawler.store.upsert;
      expect(upsert.callCount).to.be.equal(0);

      const unlock = crawler.locker.unlock;
      expect(unlock.callCount).to.be.equal(0);

      expect(crawler.queues.normal.done.callCount).to.be.equal(0);

      expect(crawler.logger.error.callCount).to.be.equal(1);
      const error = crawler.logger.error.getCall(0).args[0];
      expect(error.message).to.be.equal('cant pop');
    });
  });

  it('should handle fetch reject', () => {
    const crawler = createFullCrawler();
    sinon.stub(crawler, '_startNext', () => Q());

    // setup a good request but a server error response
    crawler.queues.normal.requests = [new Request('user', 'http://test.com/users/user1')];
    crawler.requestor.responses = [createResponse(null, 500)];
    return Q.try(() => {
      return crawler.start('test');
    }).then(() => {
      expect(crawler.queues.priority.pop.callCount).to.be.equal(1);
      expect(crawler.queues.normal.pop.callCount).to.be.equal(1);

      const lock = crawler.locker.lock;
      expect(lock.callCount).to.be.equal(1);
      expect(lock.getCall(0).args[0]).to.be.equal('http://test.com/users/user1');

      const etag = crawler.store.etag;
      expect(etag.callCount).to.be.equal(1);
      expect(etag.getCall(0).args[0]).to.be.equal('user');
      expect(etag.getCall(0).args[1]).to.be.equal('http://test.com/users/user1');

      const requestorGet = crawler.requestor.get;
      expect(requestorGet.callCount).to.be.equal(1);
      expect(requestorGet.getCall(0).args[0]).to.be.equal('http://test.com/users/user1');

      const push = crawler.queues.normal.push;
      expect(push.callCount).to.be.equal(1);
      const newRequest = push.getCall(0).args[0];
      expect(newRequest.type).to.be.equal('user');
      expect(newRequest.attemptCount).to.be.equal(1);

      const upsert = crawler.store.upsert;
      expect(upsert.callCount).to.be.equal(0);

      const unlock = crawler.locker.unlock;
      expect(unlock.callCount).to.be.equal(1);
      expect(unlock.getCall(0).args[0]).to.be.equal('lockToken');

      expect(crawler.queues.normal.done.callCount).to.be.equal(1);

      expect(crawler.logger.error.callCount).to.be.equal(1);
      const error = crawler.logger.error.getCall(0).args[0];
      expect(error.message.includes('500')).to.be.true;
    });
  });

  it('should handle process document reject', () => {
    const crawler = createFullCrawler();
    sinon.stub(crawler, '_startNext', () => Q());
    crawler.processor = { process: () => { throw new Error('bad processor') } };

    crawler.queues.normal.requests = [new Request('user', 'http://test.com/users/user1')];
    crawler.requestor.responses = [createResponse({ id: 42, repos_url: 'http://test.com/users/user1/repos' })];
    return Q.try(() => {
      return crawler.start('test');
    }).then(() => {
      expect(crawler.queues.priority.pop.callCount).to.be.equal(1);
      expect(crawler.queues.normal.pop.callCount).to.be.equal(1);

      const lock = crawler.locker.lock;
      expect(lock.callCount).to.be.equal(1);
      expect(lock.getCall(0).args[0]).to.be.equal('http://test.com/users/user1');

      const etag = crawler.store.etag;
      expect(etag.callCount).to.be.equal(1);
      expect(etag.getCall(0).args[0]).to.be.equal('user');
      expect(etag.getCall(0).args[1]).to.be.equal('http://test.com/users/user1');

      const requestorGet = crawler.requestor.get;
      expect(requestorGet.callCount).to.be.equal(1);
      expect(requestorGet.getCall(0).args[0]).to.be.equal('http://test.com/users/user1');

      const push = crawler.queues.normal.push;
      expect(push.callCount).to.be.equal(1);
      const newRequest = push.getCall(0).args[0];
      expect(newRequest.type).to.be.equal('user');
      expect(newRequest.attemptCount).to.be.equal(1);

      const upsert = crawler.store.upsert;
      expect(upsert.callCount).to.be.equal(0);

      const unlock = crawler.locker.unlock;
      expect(unlock.callCount).to.be.equal(1);
      expect(unlock.getCall(0).args[0]).to.be.equal('lockToken');

      expect(crawler.queues.normal.done.callCount).to.be.equal(1);

      expect(crawler.logger.error.callCount).to.be.equal(1);
      const error = crawler.logger.error.getCall(0).args[0];
      expect(error instanceof Error).to.be.true;
    });
  });

  it('should handle store document reject', () => {
    const crawler = createFullCrawler();
    sinon.stub(crawler, '_startNext', () => Q());
    crawler.store = { upsert: () => { throw new Error('bad upsert') } };

    crawler.queues.normal.requests = [new Request('user', 'http://test.com/users/user1')];
    crawler.requestor.responses = [createResponse({ id: 42, repos_url: 'http://test.com/users/user1/repos' })];
    return Q.try(() => {
      return crawler.start('test');
    }).then(() => {
      const unlock = crawler.locker.unlock;
      expect(unlock.callCount).to.be.equal(1);
      expect(unlock.getCall(0).args[0]).to.be.equal('lockToken');

      expect(crawler.queues.normal.done.callCount).to.be.equal(1);

      const push = crawler.queues.normal.push;
      expect(push.callCount).to.be.equal(1);
      const newRequest = push.getCall(0).args[0];
      expect(newRequest.type).to.be.equal('user');
      expect(newRequest.attemptCount).to.be.equal(1);

      expect(crawler.logger.error.callCount).to.be.equal(1);
      const error = crawler.logger.error.getCall(0).args[0];
      expect(error instanceof Error).to.be.true;
    });
  });

  it('should handle complete request reject', () => {
    const crawler = createFullCrawler();
    sinon.stub(crawler, '_startNext', () => Q());
    crawler.locker = { unlock: () => { throw new Error('bad unlock') } };

    crawler.queues.normal.requests = [new Request('user', 'http://test.com/users/user1')];
    crawler.requestor.responses = [createResponse({ id: 42, repos_url: 'http://test.com/users/user1/repos' })];
    return Q.try(() => {
      return crawler.start('test');
    }).then(() => {
      const push = crawler.queues.normal.push;
      expect(push.callCount).to.be.equal(1);
      const newRequest = push.getCall(0).args[0];
      expect(newRequest.type).to.be.equal('user');
      expect(newRequest.attemptCount).to.be.equal(1);

      expect(crawler.logger.error.callCount).to.be.equal(1);
      const error = crawler.logger.error.getCall(0).args[0];
      expect(error instanceof Error).to.be.true;
    });
  });
});

function createFullCrawler() {
  const priority = createBaseQueue();
  priority.requests = [];
  sinon.stub(priority, 'pop', () => { return Q(priority.requests.shift()); });

  const normal = createBaseQueue();
  normal.requests = [];
  sinon.stub(normal, 'pop', () => { return Q(normal.requests.shift()); });
  sinon.stub(normal, 'push', request => { return Q(); });
  sinon.spy(normal, 'done');

  const queues = createBaseQueues({ priority: priority, normal: normal });

  const locker = createBaseLocker();
  sinon.stub(locker, 'lock', request => { return Q('lockToken'); });
  sinon.stub(locker, 'unlock', request => { return Q(); });

  const store = createBaseStore();
  sinon.stub(store, 'etag', request => { return Q(); });
  sinon.stub(store, 'upsert', request => { return Q(); });

  const requestor = createBaseRequestor();
  requestor.responses = [];
  sinon.stub(requestor, 'get', () => {
    return Q(requestor.responses.shift());
  });

  const logger = createBaseLog();
  sinon.spy(logger, 'info');
  sinon.spy(logger, 'error');

  const Processor = require('../lib/processor');
  const processor = new Processor();
  sinon.spy(processor, 'process');

  const config = [];

  const result = createBaseCrawler({ queues: queues, requestor: requestor, store: store, logger: logger, locker: locker, options: config });
  result.processor = processor;
  return result;
}

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

function createBaseCrawler({queues = createBaseQueues(), store = createBaseStore(), locker = createBaseLocker, requestor = createBaseRequestor(), options = { promiseTrace: false }, logger = createBaseLog() } = {}) {
  return new Crawler(queues, store, locker, requestor, options, logger);
}

function createBaseQueues({ priority = null, normal = null, deadletter = null} = {}) {
  return new QueueSet(priority || createBaseQueue(), normal || createBaseQueue(), deadletter || createBaseQueue());
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

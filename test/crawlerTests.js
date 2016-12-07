const assert = require('chai').assert;
const chai = require('chai');
const Crawler = require('../lib/crawler');
const expect = require('chai').expect;
const extend = require('extend');
const Q = require('q');
const QueueSet = require('../lib/queueSet');
const Request = require('../lib/request');
const sinon = require('sinon');
const TraversalPolicy = require('../lib/traversalPolicy');

describe('Crawler get request', () => {
  it('should get from the priority queue first', () => {
    const priority = createBaseQueue('priority', { pop: () => { return Q(new Request('priority', 'http://test')); } });
    const normal = createBaseQueue('normal', { pop: () => { return Q(new Request('normal', 'http://test')); } });
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const locker = createBaseLocker({ lock: () => { return Q('locked'); } });
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    const requestBox = [];
    return crawler._getRequest(requestBox, { name: 'test' }).then(request => {
      expect(request.type).to.be.equal('priority');
      expect(request._originQueue === priority).to.be.true;
      expect(request.lock).to.be.equal('locked');
      expect(request.loopName).to.be.equal('test');
      expect(request).to.be.equal(requestBox[0]);
    });
  });

  it('should get from the normal queue if no priority', () => {
    const priority = createBaseQueue('priority', { pop: () => { return Q(null); } });
    const normal = createBaseQueue('normal', { pop: () => { return Q(new Request('normal', 'http://test')); } });
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const locker = createBaseLocker({ lock: () => { return Q('locked'); } });
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    const requestBox = [];
    return crawler._getRequest(requestBox, { name: 'test' }).then(request => {
      expect(request.type).to.be.equal('normal');
      expect(request._originQueue === normal).to.be.true;
      expect(request.lock).to.be.equal('locked');
      expect(request.loopName).to.be.equal('test');
      expect(request).to.be.equal(requestBox[0]);
    });
  });

  it('should return a dummy skip/delay request if none are queued', () => {
    const priority = createBaseQueue('priority', { pop: () => { return Q(null); } });
    const normal = createBaseQueue('normal', { pop: () => { return Q(null); } });
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const crawler = createBaseCrawler({ queues: queues });
    const requestBox = [];
    return crawler._getRequest(requestBox, { name: 'test' }).then(request => {
      expect(request.type).to.be.equal('_blank');
      expect(request.lock).to.be.undefined;
      expect(request.shouldSkip()).to.be.true;
      expect(request.nextRequestTime - Date.now()).to.be.approximately(2000, 4);
      expect(request.loopName).to.be.equal('test');
      expect(request).to.be.equal(requestBox[0]);
    });
  });

  it('should throw when normal pop errors', () => {
    const priority = createBaseQueue('priority', { pop: () => { return Q(null); } });
    const normal = createBaseQueue('normal', { pop: () => { throw new Error('normal test'); } });
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const crawler = createBaseCrawler({ queues: queues });
    const requestBox = [];
    return crawler._getRequest(requestBox, { name: 'test' }).then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('normal test')
    );
  });

  it('should throw when priority pop errors', () => {
    const priority = createBaseQueue('priority', { pop: () => { throw new Error('priority test'); } });
    const normal = createBaseQueue('normal', { pop: () => { return Q(null); } });
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const crawler = createBaseCrawler({ queues: queues });
    const requestBox = [];
    return crawler._getRequest(requestBox, { name: 'test' }).then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('priority test')
    );
  });

  it('should throw when acquire lock errors', () => {
    const priority = createBaseQueue('priority', { pop: () => { return Q(new Request('priority', 'http://test')); } });
    const normal = createBaseQueue('normal', { pop: () => { return Q(null); } });
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const locker = createBaseLocker({ lock: () => { throw new Error('locker error'); } });
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    const requestBox = [];
    return crawler._getRequest(requestBox, { name: 'test' }).then(
      request => assert.fail(),
      error => expect(error.message).to.be.equal('locker error')
    );
  });

  it('should abandon the request when the lock cannot be acquired', () => {
    const abandoned = [];
    const priority = createBaseQueue('priority', {
      pop: () => { return Q(new Request('priority', 'http://test')); },
      abandon: request => {
        abandoned.push(request);
        return Q();
      }
    });
    const normal = createBaseQueue('normal', { pop: () => { return Q(null); } });
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const locker = createBaseLocker({ lock: () => { return Q.reject(new Error('locker error')); } });
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    const requestBox = [];
    return crawler._getRequest(requestBox, { name: 'test' }).then(
      request => assert.fail(),
      error => {
        expect(error.message).to.be.equal('locker error');
        expect(abandoned.length).to.be.equal(1);
      });
  });

  it('should get lock error even if abandon fails', () => {
    const abandoned = [];
    const priority = createBaseQueue('priority', {
      pop: () => { return Q(new Request('priority', 'http://test')); },
      abandon: request => { throw new Error('Abandon error'); }
    });
    const normal = createBaseQueue('normal', { pop: () => { return Q(null); } });
    const queues = createBaseQueues({ priority: priority, normal: normal });
    const locker = createBaseLocker({ lock: () => { return Q.reject(new Error('locker error')); } });
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    const requestBox = [];
    return crawler._getRequest(requestBox, { name: 'test' }).then(
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
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return crawler._fetch(request).then(request => {
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
    request.policy = TraversalPolicy.update();
    let getArgs = null;
    const responses = [createResponse(null, 304, 42)];
    const requestor = createBaseRequestor({
      get: (url, options) => { getArgs = { url: url, options: options }; return Q(responses.shift()); }
    });
    const store = createBaseStore({ etag: () => { return Q(42); }, get: () => { return Q({ _metadata: {}, id: 'test' }); } });
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return crawler._fetch(request).then(request => {
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
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return crawler._fetch(request).then(request => {
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
    const crawler = createBaseCrawler({ requestor: requestor, store: store });
    return crawler._fetch(request).then(request => {
      expect(request.document).to.be.undefined;
      expect(request.shouldSkip()).to.be.true;
    });
  });

  it('should get from origin with originOnly fetch policy', () => {
    const request = new Request('foo', 'http://test');
    request.policy.fetch = 'originOnly';
    const responses = [createResponse('hey there')];
    const requestor = createBaseRequestor({ get: () => { return Q(responses.shift()); } });
    const crawler = createBaseCrawler({ requestor: requestor });
    return crawler._fetch(request).then(request => {
      expect(request.document).to.be.equal('hey there');
      expect(request.shouldSkip()).to.be.false;
    });
  });

  it('should pull from storage only storageOnly fetch policy', () => {
    const request = new Request('foo', 'http://test');
    request.policy.fetch = 'storageOnly';
    const store = createBaseStore({ get: () => { return Q({ _metadata: {}, id: 'test' }); } });
    const crawler = createBaseCrawler({ store: store });
    return crawler._fetch(request).then(request => {
      expect(request.document.id).to.be.equal('test');
      expect(request.shouldSkip()).to.be.false;
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
      error => expect(error.message.startsWith('Code 500')).to.be.true);
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
    request.policy = TraversalPolicy.update();
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
    const options = createBaseOptions();
    options.crawler.orgList = ['microsoft'];
    const crawler = createBaseCrawler({ options: options.crawler });
    expect(crawler._filter(new Request('repo', 'http://api.github.com/repo/microsoft/test')).shouldSkip()).to.be.false;
    expect(crawler._filter(new Request('repos', 'http://api.github.com/repos/microsoft/test')).shouldSkip()).to.be.false;
    expect(crawler._filter(new Request('org', 'http://api.github.com/org/microsoft/test')).shouldSkip()).to.be.false;

    expect(crawler._filter(new Request('repo', 'http://api.github.com/repo/test/test')).shouldSkip()).to.be.true;
    expect(crawler._filter(new Request('repos', 'http://api.github.com/repos/test/test')).shouldSkip()).to.be.true;
    expect(crawler._filter(new Request('org', 'http://api.github.com/org/test/test')).shouldSkip()).to.be.true;

    expect(crawler._filter(new Request('foo', 'http://api.github.com/org/test/test')).shouldSkip()).to.be.false;
  });

  it('should not filter if no config', () => {
    const options = createBaseOptions();
    const crawler = createBaseCrawler({ options: options.crawler });
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
    expect(request.nextRequestTime - Date.now()).to.be.approximately(2000, 4);
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
    const crawler = createBaseCrawler({ options: { logger: logger } });
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
    const crawler = createBaseCrawler({ options: { logger: logger } });
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
    const crawler = createBaseCrawler({ options: { logger: logger } });
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
    const crawler = createBaseCrawler({ options: { logger: logger } });
    crawler._logOutcome(newRequest);
    expect(error.length).to.be.equal(1);
    expect(error[0] instanceof Error).to.be.true;
    expect(error[0].message).to.be.equal('message');
    expect(info.length).to.be.equal(0);
  });
});

describe('Crawler queue', () => {
  it('should not queue if filtered', () => {
    const options = createBaseOptions();
    options.crawler.orgList = ['test'];
    const queue = [];
    const normal = createBaseQueue('normal', { push: request => { queue.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal });
    const request = new Request('repo', 'http://api.github.com/repo/microsoft/test');
    const crawler = createBaseCrawler({ queues: queues, options: options.crawler });
    crawler.queue(request);
    expect(request.promises.length).to.be.equal(0);
    expect(queue.length).to.be.equal(0);
  });

  it('should queue if not filtered', () => {
    const options = createBaseOptions();
    options.crawler.orgList = ['microsoft'];
    const queue = [];
    const normal = createBaseQueue('normal', { push: request => { queue.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal });
    const request = new Request('repo', 'http://api.github.com/repo/microsoft/test');
    const crawler = createBaseCrawler({ queues: queues, options: options.crawler });
    request.track(crawler.queue(request));
    expect(request.promises.length).to.be.equal(1);
    expect(queue.length).to.be.equal(1);
    expect(queue[0] !== request).to.be.true;
    expect(queue[0].type === request.type).to.be.true;
    expect(queue[0].url === request.url).to.be.true;
  });

  // TODO
  it('should queue in supplied queue', () => {
    const options = createBaseOptions();
    options.crawler.orgList = ['microsoft'];
    const queue = [];
    const normal = createBaseQueue('normal', { push: request => { queue.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal });
    const request = new Request('repo', 'http://api.github.com/repo/microsoft/test');
    const crawler = createBaseCrawler({ queues: queues, options: options.crawler });
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
    const normal = createBaseQueue('normal', { push: request => { queue.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal });
    const crawler = createBaseCrawler({ queues: queues });
    const request = new Request('test', 'http://api.github.com/repo/microsoft/test');
    request.markRequeue();
    request._originQueue = normal;
    return crawler._requeue(request).then(() => {
      // expect(request.promises.length).to.be.equal(1);
      expect(queue.length).to.be.equal(1);
      expect(queue[0] !== request).to.be.true;
      expect(queue[0].type === request.type).to.be.true;
      expect(queue[0].url === request.url).to.be.true;
      expect(queue[0].attemptCount).to.be.equal(1);
    });
  });

  it('should requeue in deadletter queue after 5 attempts', () => {
    const queue = [];
    const deadletterQueue = [];
    const normal = createBaseQueue('normal', { push: request => { queue.push(request); return Q(); } });
    const deadletter = createBaseQueue('deadletter', { push: request => { deadletterQueue.push(request); return Q(); } });
    const queues = createBaseQueues({ normal: normal, deadletter: deadletter });
    const request = new Request('test', 'http://api.github.com/repo/microsoft/test');
    request.attemptCount = 5;
    request.markRequeue();
    request._originQueue = normal;
    const crawler = createBaseCrawler({ queues: queues });
    return crawler._requeue(request).then(() => {
      expect(queue.length).to.be.equal(0);
      expect(deadletterQueue.length).to.be.equal(1);
      expect(deadletterQueue[0] !== request).to.be.true;
      expect(deadletterQueue[0].type === request.type).to.be.true;
      expect(deadletterQueue[0].url === request.url).to.be.true;
      expect(deadletterQueue[0].attemptCount).to.be.equal(6);
    });
  });
});

describe('Crawler complete request', () => {
  it('should unlock, dequeue and return the request being completed', () => {
    const done = [];
    const unlock = [];
    const normal = createBaseQueue('normal', { done: request => { done.push(request); return Q(); } });
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
    const normal = createBaseQueue('normal', {
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
    const normal = createBaseQueue('normal', { done: request => { done.push(request); return Q(); } });
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
    const normal = createBaseQueue('normal', {
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

  it('requeues and unlocks if promises fail', () => {
    const normal = createBaseQueue('normal', { push: sinon.spy(() => { return Q(); }) });
    const queues = createBaseQueues({ normal: normal });
    const locker = createBaseLocker({ unlock: sinon.spy(() => { return Q(); }) });
    const originalRequest = new Request('test', 'http://test.com');
    originalRequest.lock = 42;
    originalRequest._originQueue = normal;
    originalRequest.promises = [Q.reject(13)];
    const crawler = createBaseCrawler({ queues: queues, locker: locker });
    return crawler._completeRequest(originalRequest).then(
      request => assert.fail(),
      error => {
        expect(normal.push.callCount).to.be.equal(1);
        const requeued = normal.push.getCall(0).args[0];
        expect(requeued.type).to.be.equal(originalRequest.type);
        expect(requeued.url).to.be.equal(originalRequest.url);
        expect(locker.unlock.callCount).to.be.equal(1);
        expect(locker.unlock.getCall(0).args[0]).to.be.equal(42);
      });
  });

  it('still dequeues when unlocking fails', () => {
    const done = [];
    const unlock = [];
    const normal = createBaseQueue('normal', { done: request => { done.push(request); return Q(); } });
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
    const normal = createBaseQueue('normal', { done: () => { throw new Error('sigh'); } });
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
      return crawler._processDocument(originalRequest)
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

describe('Crawler run', () => {
  it('should panic for rejects in processOne', () => {
    const crawler = createBaseCrawler();
    sinon.stub(crawler, 'run', () => { });
    sinon.stub(crawler, 'processOne', () => { return Q.reject('test error') });
    sinon.spy(crawler, '_computeDelay');
    sinon.spy(crawler, '_panic');

    const request = new Request('user', 'http://test.com/users/user1');

    const context = { name: 'foo', delay: 0 };
    return crawler._run(context).then(() => {
      expect(crawler.processOne.callCount).to.be.equal(1);
      expect(crawler._computeDelay.callCount).to.be.equal(0);
      expect(crawler._panic.callCount).to.be.equal(1);
      expect(crawler.run.callCount).to.be.equal(1);
    }, error => {
      console.log(error);
    });
  });

  it('should panic for errors in processOne', () => {
    const crawler = createBaseCrawler();
    sinon.stub(crawler, 'run', () => { });
    sinon.stub(crawler, 'processOne', () => { throw new Error('test error') });
    sinon.spy(crawler, '_computeDelay');
    sinon.spy(crawler, '_panic');

    const request = new Request('user', 'http://test.com/users/user1');

    const context = { name: 'foo', delay: 0 };
    return crawler._run(context).then(() => {
      expect(crawler.processOne.callCount).to.be.equal(1);
      expect(crawler._computeDelay.callCount).to.be.equal(0);
      expect(crawler._panic.callCount).to.be.equal(1);
      expect(crawler.run.callCount).to.be.equal(1);
    }, error => {
      console.log(error);
    });
  });
});

describe('Crawler whole meal deal', () => {
  it('should delay starting next iteration when delay', () => {
    const crawler = createBaseCrawler();
    crawler.run = () => { };
    crawler.processOne = () => { return Q(request) };

    const request = new Request('user', 'http://test.com/users/user1');
    request.delay();

    const context = { name: 'foo', delay: 0 };
    return crawler._run(context).then(() => {
      expect(context.currentDelay).to.be.approximately(2000, 4);
    });
  });

  it('should delay starting next iteration when delayUntil', () => {
    const crawler = createBaseCrawler();
    crawler.run = () => { };
    crawler.processOne = () => { return Q(request) };

    const request = new Request('user', 'http://test.com/users/user1');
    request.delayUntil(Date.now() + 323);

    const context = { name: 'foo', delay: 0 };
    return crawler._run(context).then(() => {
      expect(context.currentDelay).to.be.approximately(323, 4);
    });
  });

  it('should delay starting next iteration when delay', () => {
    const crawler = createBaseCrawler();
    crawler.run = () => { };
    crawler.processOne = () => { return Q(request) };

    const request = new Request('user', 'http://test.com/users/user1');
    request.delay(451);

    const context = { name: 'foo', delay: 0 };
    return crawler._run(context).then(() => {
      expect(context.currentDelay).to.be.approximately(451, 10);
    });
  });

  it('should process normal requests', () => {
    const crawler = createFullCrawler();
    const normal = crawler.queues.queueTable['normal'];
    const priority = crawler.queues.queueTable['priority'];

    normal.requests = [new Request('user', 'http://test.com/users/user1')];
    crawler.requestor.responses = [createResponse({ id: 42, repos_url: 'http://test.com/users/user1/repos' })];
    return Q.try(() => { return crawler.processOne({ name: 'test' }); }).then(
      () => {
        expect(priority.pop.callCount).to.be.equal(1, 'priority call count');
        expect(normal.pop.callCount).to.be.equal(1, 'normal call count');

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

        expect(normal.done.callCount).to.be.equal(1);

        expect(crawler.logger.info.callCount).to.be.equal(1);
      },
      error => assert.fail());
  });

  it('should empty request queues', () => {
    // TODO
  });

  it('should handle getRequest reject', () => {
    const crawler = createFullCrawler();
    const priority = crawler.queues.queueTable['priority'];

    // setup a problem popping
    const normal = createBaseQueue('normal');
    sinon.stub(normal, 'pop', () => { throw Error('cant pop') });
    sinon.stub(normal, 'push', request => { return Q(); });
    sinon.spy(normal, 'done');
    // hmmm, hack in the new normal queue
    crawler.queues.queueTable['normal'] = normal;
    crawler.queues.queues[1] = normal;

    crawler.requestor.responses = [createResponse(null, 500)];
    return Q.try(() => { return crawler.processOne({ name: 'test' }); }).then(
      () => {
        expect(priority.pop.callCount).to.be.equal(1);
        expect(normal.pop.callCount).to.be.equal(1);

        const lock = crawler.locker.lock;
        expect(lock.callCount).to.be.equal(0);

        const etag = crawler.store.etag;
        expect(etag.callCount).to.be.equal(0);

        const requestorGet = crawler.requestor.get;
        expect(requestorGet.callCount).to.be.equal(0);

        const push = normal.push;
        expect(push.callCount).to.be.equal(0);

        const upsert = crawler.store.upsert;
        expect(upsert.callCount).to.be.equal(0);

        const unlock = crawler.locker.unlock;
        expect(unlock.callCount).to.be.equal(0);

        expect(normal.done.callCount).to.be.equal(0);

        expect(crawler.logger.error.callCount).to.be.equal(1);
        const error = crawler.logger.error.getCall(0).args[0];
        expect(error.message).to.be.equal('cant pop');
      },
      error => assert.fail());
  });

  it('should handle fetch reject', () => {
    const crawler = createFullCrawler();
    const normal = crawler.queues.queueTable['normal'];
    const priority = crawler.queues.queueTable['priority'];

    // setup a good request but a server error response
    normal.requests = [new Request('user', 'http://test.com/users/user1')];
    crawler.requestor.responses = [createResponse(null, 500)];
    return Q.try(() => { return crawler.processOne({ name: 'test' }); }).then(
      () => {
        expect(priority.pop.callCount).to.be.equal(1);
        expect(normal.pop.callCount).to.be.equal(1);

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

        const push = normal.push;
        expect(push.callCount).to.be.equal(1);
        const newRequest = push.getCall(0).args[0];
        expect(newRequest.type).to.be.equal('user');
        expect(newRequest.attemptCount).to.be.equal(1);

        const upsert = crawler.store.upsert;
        expect(upsert.callCount).to.be.equal(0);

        const unlock = crawler.locker.unlock;
        expect(unlock.callCount).to.be.equal(1);
        expect(unlock.getCall(0).args[0]).to.be.equal('lockToken');

        expect(normal.done.callCount).to.be.equal(1);

        expect(crawler.logger.error.callCount).to.be.equal(1);
        const error = crawler.logger.error.getCall(0).args[0];
        expect(error.message.includes('500')).to.be.true;
      },
      error => assert.fail());
  });

  it('should handle process document reject', () => {
    const crawler = createFullCrawler();
    crawler.processor = { process: () => { throw new Error('bad processor') } };
    const priority = crawler.queues.queueTable['priority'];
    const normal = crawler.queues.queueTable['normal'];

    normal.requests = [new Request('user', 'http://test.com/users/user1')];
    crawler.requestor.responses = [createResponse({ id: 42, repos_url: 'http://test.com/users/user1/repos' })];
    return Q.try(() => { return crawler.processOne({ name: 'test' }); }).then(
      () => {
        expect(priority.pop.callCount).to.be.equal(1);
        expect(normal.pop.callCount).to.be.equal(1);

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

        expect(crawler._errorHandler.callCount).to.be.equal(1);

        const push = normal.push;
        expect(push.callCount).to.be.equal(1);
        const newRequest = push.getCall(0).args[0];
        expect(newRequest.type).to.be.equal('user');
        expect(newRequest.attemptCount).to.be.equal(1);

        const upsert = crawler.store.upsert;
        expect(upsert.callCount).to.be.equal(0);

        const unlock = crawler.locker.unlock;
        expect(unlock.callCount).to.be.equal(1);
        expect(unlock.getCall(0).args[0]).to.be.equal('lockToken');

        expect(normal.done.callCount).to.be.equal(1);

        expect(crawler.logger.error.callCount).to.be.equal(1);
        const error = crawler.logger.error.getCall(0).args[0];
        expect(error instanceof Error).to.be.true;
      },
      error => assert.fail());
  });

  it('should handle store document reject', () => {
    const crawler = createFullCrawler();
    crawler.store = { upsert: () => { throw new Error('bad upsert') } };
    const normal = crawler.queues.queueTable['normal'];

    normal.requests = [new Request('user', 'http://test.com/users/user1')];
    crawler.requestor.responses = [createResponse({ id: 42, repos_url: 'http://test.com/users/user1/repos' })];
    return Q.try(() => { return crawler.processOne({ name: 'test' }); }).then(
      () => {
        const unlock = crawler.locker.unlock;
        expect(unlock.callCount).to.be.equal(1);
        expect(unlock.getCall(0).args[0]).to.be.equal('lockToken');

        expect(normal.done.callCount).to.be.equal(1);

        const push = normal.push;
        expect(push.callCount).to.be.equal(1);
        const newRequest = push.getCall(0).args[0];
        expect(newRequest.type).to.be.equal('user');
        expect(newRequest.attemptCount).to.be.equal(1);

        expect(crawler.logger.error.callCount).to.be.equal(1);
        const error = crawler.logger.error.getCall(0).args[0];
        expect(error instanceof Error).to.be.true;
      },
      error => assert.fail());
  });

  it('should handle complete request reject', () => {
    const crawler = createFullCrawler();
    crawler.locker = { unlock: () => { throw new Error('bad unlock') } };
    const normal = crawler.queues.queueTable['normal'];

    normal.requests = [new Request('user', 'http://test.com/users/user1')];
    crawler.requestor.responses = [createResponse({ id: 42, repos_url: 'http://test.com/users/user1/repos' })];
    return Q.try(() => { return crawler.processOne({ name: 'test' }); }).then(
      () => {
        const push = normal.push;
        expect(push.callCount).to.be.equal(1);
        const newRequest = push.getCall(0).args[0];
        expect(newRequest.type).to.be.equal('user');
        expect(newRequest.attemptCount).to.be.equal(1);

        expect(crawler.logger.error.callCount).to.be.equal(1);
        const error = crawler.logger.error.getCall(0).args[0];
        expect(error instanceof Error).to.be.true;
      },
      error => assert.fail());
  });
});

function createFullCrawler() {
  const priority = createBaseQueue('priority');
  priority.requests = [];
  sinon.stub(priority, 'pop', () => { return Q(priority.requests.shift()); });

  const normal = createBaseQueue('normal');
  normal.requests = [];
  sinon.stub(normal, 'pop', () => { return Q(normal.requests.shift()); });
  sinon.stub(normal, 'push', request => { return Q(); });
  sinon.stub(normal, 'done', request => { return Q(); });

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

  const options = createBaseOptions(logger);

  const result = createBaseCrawler({ queues: queues, requestor: requestor, store: store, locker: locker, options: options.crawler });
  result.processor = processor;

  sinon.spy(result, '_errorHandler');

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

function createErrorResponse(error) {
  return {
    error: new Error(error)
  };
}

function createBaseCrawler({queues = createBaseQueues(), store = createBaseStore(), locker = createBaseLocker(), requestor = createBaseRequestor(), options = createBaseOptions().crawler } = {}) {
  return new Crawler(queues, store, locker, requestor, options);
}

function createBaseOptions(logger = createBaseLog()) {
  return {
    queuing: {
      logger: logger,
      weights: [1],
      parallelPush: 10,
      attenuation: {
        ttl: 1000
      },
      tracker: {
        ttl: 6 * 60 * 1000
      }
    },
    storage: {
      logger: logger,
      ttl: 60000
    },
    locker: {
      logger: logger,
      retryCount: 3,
      retryDelay: 200
    },
    crawler: {
      logger: logger,
      processingTtl: 60 * 1000,
      promiseTrace: false,
      orgList: [],
      fetcher: {
        tokenLowerBound: 50,
        forbiddenDelay: 120000
      }
    },
    requestor: {
    }
  };
}


function createBaseQueues({ priority = null, normal = null, deadletter = null, options = null} = {}) {
  return new QueueSet([priority || createBaseQueue('priority'), normal || createBaseQueue('normal')], deadletter || createBaseQueue('deadletter'), (options || createBaseOptions()).queuing);
}

function createBaseQueue(name, { pop = null, push = null, done = null, abandon = null} = {}) {
  const result = { name: name };
  result.getName = () => { return name; };
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

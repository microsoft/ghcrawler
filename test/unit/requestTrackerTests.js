// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const assert = require('chai').assert;
const chai = require('chai');
const expect = require('chai').expect;
const extend = require('extend');
const Q = require('q');
const redlock = require('redlock');
const Request = require('ghcrawler').request;
const RequestTracker = require('../../providers/queuing/redisRequestTracker.js');
const sinon = require('sinon');

describe('NON Locking Request Tracker track', () => {
  it('should set the tag and call the operation', () => {
    const redis = createRedisClient({ get: sinon.spy((key, cb) => { cb(null, null); }), set: sinon.spy((values, cb) => { cb(null); }) });
    const locker = createNolock();
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');
    const operation = sinon.spy(() => { return Q(24); });

    return tracker.track(request, operation).then(() => {
      expect(operation.callCount).to.be.equal(1);
      expect(redis.get.callCount).to.be.equal(1);
      expect(redis.set.callCount).to.be.equal(1);
      expect(parseInt(redis.set.getCall(0).args[0][1])).to.be.approximately(Date.now(), 10);
    });
  });

  it('should reject and not call the operation if could not read tag', () => {
    const redis = createRedisClient({ get: sinon.spy((key, cb) => { cb(new Error('fail!')); }), set: sinon.spy(() => { }) });
    const locker = createNolock();
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');
    const operation = sinon.spy(() => { });

    return tracker.track(request, operation).then(
      () => assert.fail(),
      error => {
        expect(error.message).to.be.equal('fail!');
        expect(redis.get.callCount).to.be.equal(1);
        expect(operation.callCount).to.be.equal(0);
        expect(redis.set.callCount).to.be.equal(0);
      });
  });

  it('should not tag if the operation fails', () => {
    const redis = createRedisClient({ get: sinon.spy((key, cb) => { cb(null, null); }), set: sinon.spy((values, cb) => { }) });
    const locker = createNolock();
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');
    const operation = sinon.spy(() => { throw new Error('fail!'); });

    return tracker.track(request, operation).then(
      () => assert.fail(),
      error => {
        expect(error.message).to.be.equal('fail!');
        expect(operation.callCount).to.be.equal(1);
        expect(redis.get.callCount).to.be.equal(1);
        expect(redis.set.callCount).to.be.equal(0);
      });
  });

  it('should not fail if everything works and tagging fails', () => {
    const redis = createRedisClient({ get: sinon.spy((key, cb) => { cb(null, null); }), set: sinon.spy((values, cb) => { cb(new Error('fail!')); }) });
    const locker = createNolock();
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');
    const operation = sinon.spy(() => Q(24));

    return tracker.track(request, operation).then(
      result => {
        expect(result).to.be.equal(24);
        expect(operation.callCount).to.be.equal(1);
        expect(redis.get.callCount).to.be.equal(1);
        expect(redis.set.callCount).to.be.equal(1);
      });
  });

  it('should skip the operation if already tagged', () => {
    const redis = createRedisClient({ get: sinon.spy((key, cb) => { cb(null, 13); }) });
    const locker = createNolock();
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');
    const operation = sinon.spy(() => Q(24));

    return tracker.track(request, operation).then(
      result => {
        expect(result).to.not.be.equal(24);
        expect(operation.callCount).to.be.equal(0);
        expect(redis.get.callCount).to.be.equal(1);
      });
  });
});

describe('NON locking Request Tracker untrack', () => {
  it('should remove the tag', () => {
    const redis = createRedisClient({ del: sinon.spy((key, cb) => { cb(null); }) });
    const locker = createNolock();
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');

    return tracker.untrack(request).then(() => {
      expect(redis.del.callCount).to.be.equal(1);
      expect(redis.del.getCall(0).args[0].startsWith('test:')).to.be.true;
    });
  });

  it('will reject if tag removal fails', () => {
    const redis = createRedisClient({ del: sinon.spy((key, cb) => { cb(new Error('fail!')); }) });
    const locker = createNolock();
    const tracker = createTracker('test', redis, locker);
    tracker.logger = { error: sinon.spy(error => { }) };
    const request = new Request('org', 'http://test.com');

    return tracker.untrack(request).then(
      () => assert.fail(),
      error => {
        expect(error.message).to.be.equal('fail!');
        expect(tracker.logger.error.callCount).to.be.equal(1);
        expect(tracker.logger.error.getCall(0).args[0].message.startsWith('Failed')).to.be.true;
        expect(redis.del.callCount).to.be.equal(1);
        expect(redis.del.getCall(0).args[0].startsWith('test:')).to.be.true;
      });
  });
});


describe('Locking Request Tracker track', () => {
  it('should set the tag and call the operation having locked and unlocked', () => {
    const redis = createRedisClient({ get: sinon.spy((key, cb) => { cb(null, null); }), set: sinon.spy((values, cb) => { cb(null); }) });
    const locker = createRedlock();
    locker.lock = sinon.spy(() => { return Q({ value: 42 }); });
    locker.unlock = sinon.spy(lock => { return Q(); });
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');
    const operation = sinon.spy(() => { return Q(24); });

    return tracker.track(request, operation).then(() => {
      expect(locker.lock.callCount).to.be.equal(1);
      expect(locker.unlock.callCount).to.be.equal(1);
      expect(locker.unlock.getCall(0).args[0].value).to.be.equal(42);
      expect(operation.callCount).to.be.equal(1);
      expect(redis.get.callCount).to.be.equal(1);
      expect(redis.set.callCount).to.be.equal(1);
      expect(parseInt(redis.set.getCall(0).args[0][1])).to.be.approximately(Date.now(), 10);
    });
  });

  it('should reject and not attempt tagging or call the operation if could not lock', () => {
    const redis = createRedisClient({ get: sinon.spy(() => { }), set: sinon.spy(() => { }) });
    const locker = createRedlock();
    locker.lock = sinon.spy(() => { throw new redlock.LockError('fail!'); });
    locker.unlock = sinon.spy(lock => { return Q(); });
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');
    const operation = sinon.spy(() => { });

    return tracker.track(request, operation).then(
      () => assert.fail(),
      error => {
        expect(error.message).to.be.equal('fail!');
        expect(locker.lock.callCount).to.be.equal(1);
        expect(locker.unlock.callCount).to.be.equal(0);
        expect(operation.callCount).to.be.equal(0);
        expect(redis.get.callCount).to.be.equal(0);
        expect(redis.set.callCount).to.be.equal(0);
      });
  });

  it('should reject and not call the operation if could not read tag', () => {
    const redis = createRedisClient({ get: sinon.spy((key, cb) => { cb(new Error('fail!')); }), set: sinon.spy(() => { }) });
    const locker = createRedlock();
    locker.lock = sinon.spy(() => { return Q({ value: 42 }); });
    locker.unlock = sinon.spy(lock => { return Q(); });
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');
    const operation = sinon.spy(() => { });

    return tracker.track(request, operation).then(
      () => assert.fail(),
      error => {
        expect(error.message).to.be.equal('fail!');
        expect(locker.lock.callCount).to.be.equal(1);
        expect(locker.unlock.callCount).to.be.equal(1);
        expect(locker.unlock.getCall(0).args[0].value).to.be.equal(42);
        expect(redis.get.callCount).to.be.equal(1);
        expect(operation.callCount).to.be.equal(0);
        expect(redis.set.callCount).to.be.equal(0);
      });
  });

  it('should not tag if the operation fails', () => {
    const redis = createRedisClient({ get: sinon.spy((key, cb) => { cb(null, null); }), set: sinon.spy((values, cb) => { }) });
    const locker = createRedlock();
    locker.lock = sinon.spy(() => { return Q({ value: 42 }); });
    locker.unlock = sinon.spy(() => { return Q(); });
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');
    const operation = sinon.spy(() => { throw new Error('fail!'); });

    return tracker.track(request, operation).then(
      () => assert.fail(),
      error => {
        expect(error.message).to.be.equal('fail!');
        expect(locker.lock.callCount).to.be.equal(1);
        expect(operation.callCount).to.be.equal(1);
        expect(locker.unlock.callCount).to.be.equal(1);
        expect(locker.unlock.getCall(0).args[0].value).to.be.equal(42);
        expect(redis.get.callCount).to.be.equal(1);
        expect(redis.set.callCount).to.be.equal(0);
      });
  });

  it('should not fail if everything works and tagging fails', () => {
    const redis = createRedisClient({ get: sinon.spy((key, cb) => { cb(null, null); }), set: sinon.spy((values, cb) => { cb(new Error('fail!')); }) });
    const locker = createRedlock();
    locker.lock = sinon.spy(() => { return Q({ value: 42 }); });
    locker.unlock = sinon.spy(() => { return Q(); });
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');
    const operation = sinon.spy(() => Q(24));

    return tracker.track(request, operation).then(
      result => {
        expect(result).to.be.equal(24);
        expect(locker.lock.callCount).to.be.equal(1);
        expect(operation.callCount).to.be.equal(1);
        expect(locker.unlock.callCount).to.be.equal(1);
        expect(locker.unlock.getCall(0).args[0].value).to.be.equal(42);
        expect(redis.get.callCount).to.be.equal(1);
        expect(redis.set.callCount).to.be.equal(1);
      });
  });

  it('should not fail if everything works and unlock fails', () => {
    const redis = createRedisClient({ get: sinon.spy((key, cb) => { cb(null, null); }), set: sinon.spy((values, cb) => { cb(new Error('fail!')); }) });
    const locker = createRedlock();
    locker.lock = sinon.spy(() => { return Q({ value: 42 }); });
    locker.unlock = sinon.spy(() => { throw new Error('fail!'); });
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');
    const operation = sinon.spy(() => Q(24));

    return tracker.track(request, operation).then(
      result => {
        expect(result).to.be.equal(24);
        expect(locker.lock.callCount).to.be.equal(1);
        expect(operation.callCount).to.be.equal(1);
        expect(locker.unlock.callCount).to.be.equal(1);
        expect(locker.unlock.getCall(0).args[0].value).to.be.equal(42);
        expect(redis.get.callCount).to.be.equal(1);
        expect(redis.set.callCount).to.be.equal(1);
      });
  });

  it('should skip the operation if already tagged', () => {
    const redis = createRedisClient({ get: sinon.spy((key, cb) => { cb(null, 13); }) });
    const locker = createRedlock();
    locker.lock = sinon.spy(() => { return Q({ value: 42 }); });
    locker.unlock = sinon.spy(() => { return Q(); });
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');
    const operation = sinon.spy(() => Q(24));

    return tracker.track(request, operation).then(
      result => {
        expect(result).to.not.be.equal(24);
        expect(locker.lock.callCount).to.be.equal(1);
        expect(operation.callCount).to.be.equal(0);
        expect(locker.unlock.callCount).to.be.equal(1);
        expect(locker.unlock.getCall(0).args[0].value).to.be.equal(42);
        expect(redis.get.callCount).to.be.equal(1);
      });
  });
});

describe('Request Tracker untrack', () => {
  it('should remove the tag having locked and unlocked', () => {
    const redis = createRedisClient({ del: sinon.spy((key, cb) => { cb(null); }) });
    const locker = createRedlock();
    locker.lock = sinon.spy(() => { return Q({ value: 42 }); });
    locker.unlock = sinon.spy(lock => { return Q(); });
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');

    return tracker.untrack(request).then(() => {
      expect(locker.lock.callCount).to.be.equal(1);
      expect(locker.unlock.callCount).to.be.equal(1);
      expect(locker.unlock.getCall(0).args[0].value).to.be.equal(42);
      expect(redis.del.callCount).to.be.equal(1);
      expect(redis.del.getCall(0).args[0].startsWith('test:')).to.be.true;
    });
  });

  it('will reject and not remove the tag if locking fails', () => {
    const redis = createRedisClient({ del: sinon.spy((key, cb) => { cb(null); }) });
    const locker = createRedlock();
    locker.lock = sinon.spy(() => { throw new redlock.LockError('fail!'); });
    locker.unlock = sinon.spy(lock => { return Q(); });
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');

    return tracker.untrack(request).then(
      () => assert.fail(),
      error => {
        expect(error.message).to.be.equal('fail!');
        expect(locker.lock.callCount).to.be.equal(1);
        expect(locker.unlock.callCount).to.be.equal(0);
        expect(redis.del.callCount).to.be.equal(0);
      });
  });

  it('will reject if tag removal fails', () => {
    const redis = createRedisClient({ del: sinon.spy((key, cb) => { cb(new Error('fail!')); }) });
    const locker = createRedlock();
    locker.lock = sinon.spy(() => { return Q({ value: 42 }); });
    locker.unlock = sinon.spy(lock => { return Q(); });
    const tracker = createTracker('test', redis, locker);
    tracker.logger = { error: sinon.spy(error => { }) };
    const request = new Request('org', 'http://test.com');

    return tracker.untrack(request).then(
      () => assert.fail(),
      error => {
        expect(error.message).to.be.equal('fail!');
        expect(tracker.logger.error.callCount).to.be.equal(1);
        expect(tracker.logger.error.getCall(0).args[0].message.startsWith('Failed')).to.be.true;
        expect(locker.lock.callCount).to.be.equal(1);
        expect(locker.unlock.callCount).to.be.equal(1);
        expect(locker.unlock.getCall(0).args[0].value).to.be.equal(42);
        expect(redis.del.callCount).to.be.equal(1);
        expect(redis.del.getCall(0).args[0].startsWith('test:')).to.be.true;
      });
  });

  it('will resolve and remove the tag even if unlock fails', () => {
    const redis = createRedisClient({ del: sinon.spy((key, cb) => { cb(null); }) });
    const locker = createRedlock();
    locker.lock = sinon.spy(() => { return Q({ value: 42 }); });
    locker.unlock = sinon.spy(lock => { throw new redlock.LockError('fail!'); });
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');

    return tracker.untrack(request).then(
      result => {
        expect(locker.lock.callCount).to.be.equal(1);
        expect(locker.unlock.callCount).to.be.equal(1);
        expect(locker.unlock.getCall(0).args[0].value).to.be.equal(42);
        expect(redis.del.callCount).to.be.equal(1);
        expect(redis.del.getCall(0).args[0].startsWith('test:')).to.be.true;
      },
      error =>
        assert.fail()
    );
  });
});

describe('Request Tracker concurrency', () => {
  it('should remove the tag having locked and unlocked', () => {
    const getResponses = [null, 13];
    const redis = createRedisClient({
      get: delaySpy((key, cb) => { cb(null, getResponses.shift()); }),
      set: delaySpy((values, cb) => { cb(null); }),
      del: delaySpy((key, cb) => { cb(null); })
    });
    const locker = createRedlock();
    const lockResponses = [{ value: 42 }, null]
    locker.lock = delaySpy(() => { if (lockResponses.shift) return Q({ value: 42 }); else throw new Error('fail'); });
    locker.unlock = delaySpy(lock => { return Q(); });
    const tracker = createTracker('test', redis, locker);
    const request = new Request('org', 'http://test.com');
    const operation = sinon.spy(() => Q(24));

    const path1 = tracker.track(request, operation);
    const path2 = tracker.track(request, operation);

    return Q.all([path1, path2]).spread((one, two) => {
      expect(locker.lock.callCount).to.be.equal(2);
      expect(locker.unlock.callCount).to.be.equal(2);
      expect(operation.callCount).to.be.equal(1);
      expect(redis.get.callCount).to.be.equal(2);
      expect(redis.set.callCount).to.be.equal(1);
      expect(parseInt(redis.set.getCall(0).args[0][1])).to.be.approximately(Date.now(), 10);
    });
  });
});

// TODO, yes we should theoretically be able to use "arguments" here but it seems to be
// binding to the factory method arguments, not the runtime arguments.  Doing this for now.
function delaySpy(f, time = 2) {
  if (f.length === 0)
    return delaySpy0(f, time);
  if (f.length === 1)
    return delaySpy1(f, time);
  if (f.length === 2)
    return delaySpy2(f, time);
}

function delaySpy0(f, time = 2) {
  return sinon.spy(() => {
    const self = this;
    return Q.delay(time).then(() => { return f.apply(self, []); });
  });
}

function delaySpy1(f, time = 2) {
  return sinon.spy(x => {
    const self = this;
    if (typeof x === 'function') {
      setTimeout(() => { f.apply(self, [x]); }, time);
    } else {
      return Q.delay(time).then(() => { return f.apply(self, [x]); });
    }
  });
}

function delaySpy2(f, time = 2) {
  return sinon.spy((x, y) => {
    const self = this;
    if (typeof y === 'function') {
      setTimeout(() => { f.apply(self, [x, y]); }, time);
    } else {
      return Q.delay(time).then(() => { return f.apply(self, [x, y]); });
    }
  });
}

function createRedisClient({ get = null, set = null, del = null } = {}) {
  const result = {};
  result.get = get || (() => assert.fail('should not lock'));
  result.set = set || (() => assert.fail('should not extend'));
  result.del = del || (() => assert.fail('should not unlock'));
  return result;
}

function createRedlock({ lock = null, extend = null, unlock = null } = {}) {
  const result = {};
  result.lock = lock || (() => assert.fail('should not lock'));
  result.extend = extend || (() => assert.fail('should not extend'));
  result.unlock = unlock || (() => assert.fail('should not unlock'));
  return result;
}

function createNolock() {
  const result = {};
  result.lock = () => null;
  result.extend = extend || (() => assert.fail('should not extend'));
  result.unlock = () => { };
  return result;
}

function createTracker(prefix, redisClient = createRedisClient(), locker = createNolock(), options = createOptions()) {
  return new RequestTracker(prefix, redisClient, locker, options);
}

function createOptions() {
  return {
    logger: createBaseLog(),
    tracker: {
      lockTtl: 1000,
      ttl: 6 * 60 * 1000
    }
  };
}

function createBaseLog({ info = null, warn = null, error = null, verbose = null, silly = null } = {}) {
  const result = {};
  result.info = info || (() => { });
  result.warn = warn || (() => { });
  result.error = error || (() => { });
  result.verbose = verbose || ((message) => { console.log(message) });
  result.silly = silly || ((message) => { console.log(message) });
  result.level = 'silly';
  return result;
}
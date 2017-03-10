// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const NestedQueue = require('./nestedQueue');
const Q = require('q');
const qlimit = require('qlimit');
const debug = require('debug')('crawler:queuing:trackedqueue');
debug.log = console.info.bind(console);

class TrackedQueue extends NestedQueue {
  constructor(queue, tracker, options) {
    super(queue);
    this.tracker = tracker;
    this.options = options;
    this.logger = options.logger;
  }

  push(requests) {
    debug('push: enter');
    const self = this;
    requests = Array.isArray(requests) ? requests : [requests];
    return Q.all(requests.map(qlimit(self.options.parallelPush || 1)(request => {
      return self.tracker.track(request, self.queue.push.bind(self.queue));
    }))).then(result => {
      debug('push: exit (success)');
      return result;
    });
  }

  pop() {
    debug('pop: enter');
    const self = this;
    return this.queue.pop().then(request => {
      if (!request) {
        debug('pop: exit (no request)');
        return null;
      }
      return self.tracker.untrack(request).then(
        () => {
          debug('pop: exit (untracked)');
          return request;
        },
        error => {
          // if we cannot untrack, abandon the popped message and fail the pop.
          return self.abandon(request).finally(() => {
            debug('pop: exit (abandoned)');
            throw error;
          });
        });
    });
  }

  flush() {
    debug('flush: enter');
    const self = this;
    return this.tracker.flush().then(() => {
      return self.queue.flush();
    }).then(result => {
      debug('flush: exit (success)');
      return result;
    });
  }
}

module.exports = TrackedQueue;
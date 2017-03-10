// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

class NestedQueue {
  constructor(queue) {
    this.queue = queue;
  }

  push(requests) {
    return this.queue.push(requests);
  }

  pop() {
    return this.queue.pop();
  }

  done(request) {
    return this.queue.done(request);
  }

  defer(request) {
    return this.queue.defer(request);
  }

  abandon(request) {
    return this.queue.abandon(request);
  }

  subscribe() {
    return this.queue.subscribe();
  }

  unsubscribe() {
    return this.queue.unsubscribe();
  }

  flush() {
    return this.queue.flush();
  }

  getInfo() {
    return this.queue.getInfo();
  }

  getName() {
    return this.queue.getName();
  }
}

module.exports = NestedQueue;
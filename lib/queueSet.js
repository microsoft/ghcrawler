// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Q = require('q');

class QueueSet {
  constructor(queues, deadletter, options) {
    this.queues = queues;
    this.queueTable = queues.reduce((table, queue) => {
      table[queue.getName()] = queue;
      return table;
    }, {});
    if (this.queues.length > Object.getOwnPropertyNames(this.queueTable).length) {
      throw new Error('Duplicate queue names');
    }
    this.deadletter = deadletter;
    this.options = options;
    this.options._config.on('changed', this._reconfigure.bind(this));
    this.startMap = this._createStartMap(this.options.weights || [1]);
    this.popCount = 0;
  }

  _reconfigure(current, changes) {
    if (changes.some(patch => patch.path === '/weights')) {
      this._startMap = this._createStartMap(this.options.weights || [1]);
    }
    return Q();
  }

  push(requests, name) {
    return this.getQueue(name).push(requests);
  }

  pushDead(requests) {
    return this.deadletter.push(requests);
  }

  repush(original, newRequest) {
    const queue = original._retryQueue ? this.getQueue(original._retryQueue) : original._originQueue;
    return queue.push(newRequest);
  }

  subscribe() {
    return Q.all(this.queues.concat([this.deadletter]).map(queue => { return queue.subscribe(); }));
  }

  unsubscribe() {
    return Q.all(this.queues.concat([this.deadletter]).map(queue => { return queue.unsubscribe(); }));
  }

  pop(startMap = this.startMap) {
    let result = Q();
    const start = startMap[this.popCount++ % startMap.length];
    for (let i = 0; i < this.queues.length; i++) {
      const queue = this.queues[(start + i) % this.queues.length];
      result = result.then(this._pop.bind(this, queue));
    }
    return result;
  }

  _pop(queue, request = null) {
    return Q.try(() => {
      return request ? request : queue.pop();
    }).then(result => {
      if (result && !result._originQueue) {
        result._originQueue = queue;
      }
      return result;
    });
  }

  done(request) {
    const acked = request.acked;
    request.acked = true;
    return !acked && request._originQueue ? request._originQueue.done(request) : Q();
  }

  defer(request) {
    return request._originQueue ? request._originQueue.defer(request) : Q();
  }

  abandon(request) {
    const acked = request.acked;
    request.acked = true;
    return !acked && request._originQueue ? request._originQueue.abandon(request) : Q();
  }

  getQueue(name) {
    const result = name === 'deadletter' ? this.deadletter : this.queueTable[name];
    if (!result) {
      throw new Error(`Queue not found: ${name}`);
    }
    return result;
  }

  _createStartMap(weights) {
    if (this.queues.length < weights.length) {
      throw new Error('Cannot have more weights than queues');
    }
    const result = [];
    for (let i = 0; i < weights.length; i++) {
      for (let j = 0; j < weights[i]; j++) {
        result.push(i);
      }
    }
    if (result.length === 0) {
      throw new Error('Weights must not be empty');
    }
    return result;
  }
}

module.exports = QueueSet;
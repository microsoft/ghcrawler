const jsonpatch = require('fast-json-patch');
const Q = require('q');
const qlimit = require('qlimit');

class QueueSet {
  constructor(queues, deadletter, options = null) {
    this.queues = queues;
    this.queueTable = queues.reduce((table, queue) => {
      table[queue.name] = queue;
      return table;
    }, {});
    if (this.queues.length > Object.getOwnPropertyNames(this.queueTable).length) {
      throw new Error('Duplicate queue names');
    }
    this.deadletter = deadletter;
    this.options = options || { weights: [1] };
    this.options.reconfigure = this.reconfigure.bind(this);
    this.startMap = this._createStartMap(this.options.weights);
    this.popCount = 0;
  }

  reconfigure(patches) {
    // remember options that need processing, apply and then do any necessary processing
    const currentWeights = this.options.weights;
    jsonpatch.apply(this.options, patches);
    if (currentWeights !== this.options.weights) {
      this._startMap = this._createStartMap(this.options.weights);
    }
    return Q();
  }

  push(requests, name) {
    return this._findQueue(name).push(requests);
  }

  pushDead(requests) {
    return this.deadletter.push(requests);
  }

  repush(original, newRequest) {
    return original._originQueue.push(newRequest);
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

  abandon(request) {
    const acked = request.acked;
    request.acked = true;
    return !acked && request._originQueue ? request._originQueue.abandon(request) : Q();
  }

  _findQueue(name) {
    const result = this.queueTable[name];
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
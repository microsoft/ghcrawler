const Q = require('q');

class QueueSet {
  constructor(queues, deadletter, options, queuesMetrics) {
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
    this.options.on('change', this._reconfigure.bind(this));
    this.startMap = this._createStartMap(this.options.weights || [1]);
    this.popCount = 0;
    this.metrics = queuesMetrics;
  }

  _reconfigure(patches) {
    if (patches.some(patch => patch.path === '/weights')) {
      this._startMap = this._createStartMap(this.options.weights || [1]);
    }
    return Q();
  }

  push(requests, name) {
    this._incrementMetricsCounter(name, 'push');
    return this._findQueue(name).push(requests);
  }

  pushDead(requests) {
    return this.deadletter.push(requests);
  }

  repush(original, newRequest) {
    const queue = original._retryQueue ? this._findQueue(original._retryQueue) : original._originQueue;
    this._incrementMetricsCounter(queue.getName(), 'repush');
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
    if (acked || !request._originQueue) {
      return Q();
    }
    this._incrementMetricsCounter(request._originQueue.getName(), 'done');
    return request._originQueue.done(request);
  }

  abandon(request) {
    const acked = request.acked;
    request.acked = true;
    if (acked || !request._originQueue) {
      return Q();
    }
    this._incrementMetricsCounter(request._originQueue.getName(), 'abandon');
    return request._originQueue.abandon(request);
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

  _incrementMetricsCounter(queueName, operation) {
    if (this.metrics && this.metrics[queueName] && this.metrics[queueName][operation]) {
      const counter = this.metrics[queueName][operation];
      counter.incr();
    }
  }
}

module.exports = QueueSet;
const Q = require('q');
const qlimit = require('qlimit');

class QueueSet {
  constructor(priority, normal, deadletter) {
    this.priority = priority;
    this.normal = normal;
    this.deadletter = deadletter;
    this.allQueues = [priority,normal, deadletter];
  }

  pushPriority(requests) {
    return this.push(requests, this.priority);
  }

  push(requests, queue = this.normal) {
    return queue.push(requests);
  }

  pushDead(requests) {
    return this.push(requests, this.deadletter);
  }

  repush(original, newRequest) {
    return this.push(newRequest, original._originQueue);
  }

  subscribe() {
    return Q.all(this.allQueues.map(queue => { return queue.subscribe(); }));
  }

  unsubscribe() {
    return Q.all(this.allQueues.map(queue => { return queue.unsubscribe(); }));
  }

  pop() {
    return Q()
      .then(this._pop.bind(this, this.priority))
      .then(this._pop.bind(this, this.normal));
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
    return request._originQueue ? request._originQueue.done(request) : Q();
  }

  abandon(request) {
    return request._originQueue ? request._originQueue.abandon(request) : Q();
  }
}

module.exports = QueueSet;
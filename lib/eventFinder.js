const Q = require('q');
const async = require('async');

class EventFinder {
  constructor(requestor, eventStore) {
    this.requestor = requestor;
    this.eventStore = eventStore;
  }

  discoverAndQueue(eventSource, eventSink) {
    if (!eventSource) {
      return Q(null);
    }
    const self = this;
    return this.getNewEvents(eventSource).then(events => {
      return self._queueEvents(events, eventSink);
    });
  }

  _queueEvents(events, eventSink) {
    const deferred = Q.defer();
    const work = (event, cb) => { eventSink.push(event.type, event.url, { payload: event }).then(() => { cb(); }); };
    const done = this._resolveReject(deferred);
    async.eachLimit(events, 10, work, done);
    return deferred.promise;
  }

  getNewEvents(eventSource) {
    const self = this;
    return this.requestor.getAll(eventSource).then(self._findNew.bind(self));
  }

  _findNew(events) {
    const deferred = Q.defer();
    const work = (event, cb) => { this.eventStore.etag('event', event.url, (err, tag) => { cb(err, !tag); }); };
    const done = this._resolveReject(deferred);
    async.filterLimit(events, 10, work, done);
    return deferred.promise;
  }

  _resolveReject(deferred) {
    return (err, value) => {
      if (err)
        deferred.reject(err);
      else
        deferred.resolve(value);
    };
  }
}
module.exports = EventFinder;
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
    return this.getNewEvents(eventSource).then(events => {
      self._queueEvents(events, eventSink);
    });
  }

  _queueEvents(events, eventSink) {
    events.forEach(event => {
      eventSink.push({ type: 'event', url: event.url });
    });
  }

  getNewEvents(eventSource) {
    const self = this;
    return this.requestor.getAll(eventSource).then(self._findNew.bind(self));
  }

  _findNew(events, callback = null) {
    const deferred = Q.defer();
    const realCallback = callback || ((err, value) => {
      if (err)
        deferred.reject(err);
      else
        deferred.resolve(value);
    });
    async.filterLimit(events, 10, (event, cb) => {
      this.eventStore.etag('event', event.url, (err, tag) => { cb(err, !tag); });
    }, realCallback);
    return callback ? null : deferred.promise;
  }
}
module.exports = EventFinder;
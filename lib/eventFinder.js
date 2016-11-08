const Q = require('q');
const qlimit = require('qlimit');

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
    const limit = qlimit(10);
    return Q.all(events.each(limit(event => {
      eventSink.push(event.type, event.url, { payload: event });
    })));
  }

  getNewEvents(eventSource) {
    const self = this;
    return this.requestor.getAll(eventSource).then(self._findNew.bind(self));
  }

  _findNew(events) {
    const limit = qlimit(10);
    return Q.all(events.filter(limit(event => {
      return !this.eventStore.etag('event', event.url, (err, tag));
    })));
  }
}
module.exports = EventFinder;
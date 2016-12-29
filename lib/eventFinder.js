// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

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
    return this.requestor.getAll(eventSource).then(this._findNew.bind(this));
  }

  // Find the events for which we do NOT have a document.
  _findNew(events) {
    const self = this;
    return Q.all(events.map(qlimit(10)(event => {
      return self.eventStore.etag('event', event.url).then(etag => {
        return etag ? null : event;
      });
    }))).then(events => {
      return events.filter(event =>event);
    });
  }
}

module.exports = EventFinder;
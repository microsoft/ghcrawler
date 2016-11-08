const async = require('async');

class WebhookDriver {
  constructor(trigger, eventFinder, eventSink) {
    this.trigger = trigger;
    this.eventFinder = eventFinder;
    this.eventSink = eventSink;
  }

  start() {
    return this.trigger.pop()
      .then(this._handleRequest.bind(this))
      .then(this._deleteFromQueue.bind(this))
      .then(this._startNext.bind(this));
  }

  _startNext() {
    setTimeout(this.start.bind(this), 0);
  }

  _handleRequest(request) {
    const source = this._chooseSource(request);
    return this.eventFinder.discoverAndQueue(source, this.eventSink).then(() => {
      return request;
    });
  }

  _deleteFromQueue(request) {
    return this.trigger.done(request).then(() => { return request; });
  }

  _chooseSource(request) {
    if (!request.qualifier) {
      return null;
    }
    if (request.qualifier.includes('/')) {
      return `https://api.github.com/repos/${request.qualifier}/events`;
    }
    return `https://api.github.com/orgs/${request.qualifier}/events`;
  }
}
module.exports = WebhookDriver;
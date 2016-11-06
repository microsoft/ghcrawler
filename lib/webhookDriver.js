const async = require('async');

class WebhookDriver {

  static watch(trigger, eventFinder, eventSink) {
    async.whilst(
      () => true,
      (completion) => {
        setTimeout(() => { WebhookDriver.handleNext(trigger, eventFinder, eventSink, completion); }, 0);
      });
  }

  static handleNext(trigger, eventFinder, eventSink, completion) {
    trigger.pop(message => {
      const source = WebhookDriver._chooseSource(message);
      eventFinder.discoverAndQueue(source, eventSink).then(
        () => { completion(); },
        (err) => { completion(err); });
    });
  }

  static _chooseSource(message) {
    if (repoEvents.has(message.type)) {
      return `https://api.github.com/repos/${message.qualifier}/events`;
    }
    if (orgEvents.has(type)) {
      return `https://api.github.com/orgs/${message.qualifier}/events`;
    }
    return null;
  }
}
module.exports = WebhookDriver;
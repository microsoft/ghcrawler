const async = require('async');

const repoEvents = new Set(['issues', 'issue_comment', 'push', 'status']);
const orgEvents = new Set(['membership']);

class WebhookDriver {

  static watch(queue, eventFinder, eventSink) {
    async.whilst(
      () => true,
      (completion) => {
        setTimeout(() => { WebhookDriver.handleNext(queue, eventFinder, eventSink, completion); }, 0);
      });
  }

  static handleNext(queue, eventFinder, eventSink, completion) {
    queue.pop(message => {
      const source = WebhookDriver._chooseSource(message);
      eventFinder.discoverAndQueue(source, eventSink).then(
        () => { completion(); },
        (err) => { completion(err); });
    });
  }

  static _chooseSource(message) {
    // TODO this top bit relies on service bus message structure
    const type = message.customProperties.event;
    const event = JSON.parse(message.body);
    if (repoEvents.has(type)) {
      const name = event.repository.full_name;
      return `https://api.github.com/repos/${name}/events`;
    } else if (orgEvents.has(type)) {
      const name = event.organization.login.toLowercase();
      return `https://api.github.com/orgs/${name}/events`;
    }
    return null;
  }
}
module.exports = WebhookDriver;
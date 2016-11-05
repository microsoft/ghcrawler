const azure = require('azure');

const topicName = 'webhookevents';
const subscriptionName = 'ghcrawlerdev';
const notSoSmartTimeoutMilliseconds = 1000;

class ServiceBusQueue {

  constructor(connectionSpec) {
    this.bus = azure.createServiceBusService(connectionSpec);
  }

  pop(handler) {
    this.bus.receiveSubscriptionMessage(topicName, subscriptionName, { isPeekLock: true }, (peekError, message) => {
      if (!message) {
        // No messages found. Let's chill out for a little bit.
        // Could use a smart retry system here based on how many chillout moments we have had lately.
        return setTimeout(completion, notSoSmartTimeoutMilliseconds);
      }
      if (peekError) {
        return;
      }
      return handler(message);
    });
  }
}
module.exports = ServiceBusQueue;
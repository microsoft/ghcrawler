// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const amqp10 = require('amqp10');
const moment = require('moment');
const Q = require('q');
const qlimit = require('qlimit');

const AmqpPolicy = amqp10.Policy;

class Amqp10Queue {
  constructor(client, name, queueName, formatter, manager, options) {
    this.debug = require('debug')(`crawler:queuing:ampq10:${queueName}`);
    this.debug.log = console.info.bind(console);
    this.client = client;
    this.name = name;
    this.queueName = queueName;
    this.messageFormatter = formatter;
    this.manager = manager;
    this.options = options;
    this.logger = options.logger;
    this.mode = { receive: 'receive', send: 'send' };
    this.currentAmqpCredit = options.credit || 10;
    this.options._config.on('changed', this._reconfigure.bind(this));

    this.receiver = null;
    this.sender = null;
    this.messages = [];
  }

  subscribe() {
    this._silly('subscribe: enter');
    if (this.receiver && this.sender) {
      this._silly('subscribe: exit (no receiver or sender)');
      return Q();
    }
    const receive = this.mode.receive === 'receive';
    const send = this.mode.send === 'send';
    return this.client.then(client => {
      const queuePromise = this.manager ? this.manager.createQueue(this.queueName) : Q();
      return queuePromise.then(() => {
        const size = (this.options.messageSize || 200) * 1024;
        const basePolicy = {
          senderLink: { attach: { maxMessageSize: size } },
          receiverLink: { attach: { maxMessageSize: size } }
        };
        const receivePolicy = AmqpPolicy.Utils.RenewOnSettle(this.currentAmqpCredit || 10, 1, basePolicy).receiverLink;
        return Q.spread([
          receive ? client.createReceiver(this.queueName, receivePolicy) : Q(null),
          send ? client.createSender(this.queueName, basePolicy.senderLink) : Q(null)
        ], (receiver, sender) => {
          this.logger.info(`Connecting to ${this.queueName}`);
          if (sender) {
            this.sender = sender;
            sender.on('errorReceived', err => {
              this._logReceiverSenderError(err, 'sender');
            });
            sender.on('attached', () => {
              this.logger.info(`Sender attached to ${this.queueName}`);
            });
            sender.on('detached', () => {
              this.logger.info(`Sender detached from ${this.queueName}`);
            });
          }
          if (receiver) {
            this.receiver = receiver;
            receiver.on('message', message => {
              this._silly('receiver: message received');
              this.messages.push(message);
            });
            receiver.on('errorReceived', err => {
              this._logReceiverSenderError(err, 'receiver');
            });
            receiver.on('attached', () => {
              this.logger.info(`Receiver attached to ${this.queueName}`);
            });
            receiver.on('detached', () => {
              this.logger.info(`Receiver detached from ${this.queueName}`);
            });
          }
          process.once('SIGINT', () => {
            this._silly('client: disconnecting due to SIGINT');
            client.disconnect();
          });
          this._silly('subscribe: exit');
          return Q();
        });
      });
    }).catch(error => {
      this.logger.error(`${this.queueName} could not be instantiated. Error: ${error}`);
      this._silly('subscribe: exit (error)');
    });
  }

  unsubscribe() {
    this._silly('unsubscribe: enter');
    this.logger.info(`Detaching from ${this.queueName}`);
    if (this.sender) {
      this._silly('unsubscribe: detaching sender');
      this.sender.detach({ closed: true });
    }
    if (this.receiver) {
      this._silly('unsubscribe: detaching receiver');
      this.receiver.detach({ closed: true });
    }
    this.receiver = null;
    this.sender = null;
    this.messages = [];
    this._silly('unsubscribe: exit');
    return Q();
  }

  push(requests) {
    this._silly('push: enter');
    if (!this.sender) {
      this._silly('push: exit (no sender)');
      return Q();
    }
    requests = Array.isArray(requests) ? requests : [requests];
    this._silly(`push: pushing ${requests.length} requests`);
    let body = null;
    return Q.all(requests.map(qlimit(this.options.parallelPush || 1)(request => {
      this._incrementMetric('push');
      this._silly(`push: ${request.type} ${request.url} (state: ${this.sender.state()})`);
      body = JSON.stringify(request);
      return this.sender.send(body);
    }))).then(
      result => {
        this._silly('push: exit');
        return result;
      },
      error => {
        // if there was as force detach, a reattach should come real soon so try resending
        // after a short delay.
        if (error.message && error.message.includes('force')) {
          return Q.delay(500).then(() => this.sender.send(body));
        }
        throw error;
      });
  }

  pop() {
    this._silly('pop: enter');
    const message = this._findMessage();
    if (!message || !message.body || !this.receiver) {
      this._silly('pop: exit (nothing to pop)');
      return Q(null);
    }
    this._incrementMetric('pop');
    const request = this.messageFormatter(message);
    if (!request) {
      // We are never going to process this message (no formatter).  Make sure to accept the message to
      // ensure the queuing system gives back the credits.
      this._accept(message, 'pop');
      this._silly('pop: exit (message formatter returned null)')
      return Q(null);
    }
    request._message = message;
    this._silly(`pop: exit (${request.type} ${request.url})`);
    return Q(request);
  }

  _findMessage() {
    this._silly('_findMessage: enter');
    // Clean up and trim off any messages that have actually expired according to the queuing system
    const now = moment();
    const validIndex = this.messages.findIndex(message => now.isBefore(message.messageAnnotations['x-opt-locked-until']));
    if (validIndex < 0) {
      this._silly('_findMessage: exit (all expired)');
      return null;
    }
    // remove any expired messages.  Make sure to release them so the AMPQ client does the proper accounting and sends more messages.
    const expired = this.messages.splice(0, validIndex);
    if (expired && expired.length > 0) {
      this.logger.info(`Releasing ${expired.length} expired messages from ${this.queueName}.`);
      expired.forEach(message => this._release(message, 'pop'));
    }
    // Find a candidate message -- one that is not expired or deferred
    const candidateIndex = this.messages.findIndex(message =>
      now.isBefore(message.messageAnnotations['x-opt-locked-until']) && (!message._deferUntil || message._deferUntil.isBefore(now)));
    if (candidateIndex < 0) {
      this._silly('_findMessage: exit (all expired or deferred)');
      return null;
    }
    const result = this.messages[candidateIndex];
    this.messages.splice(candidateIndex, 1);
    this._silly('_findMessage: exit');
    return result;
  }

  _release(message, caller) {
    this._silly('_release: enter');
    try {
      return Q(this.receiver.release(message)).then(result => {
        this._silly('_release: exit');
        return result;
      });
    } catch (error) {
      this.logger.info(`Could not release message for ${this.queueName}. Caller: ${caller} Error: ${error.message}`);
      this._silly('_release: exit (error)');
      return Q();
    }
  }

  _accept(message, caller) {
    this._silly('_accept: enter');
    try {
      return Q(this.receiver.accept(message)).then(result => {
        this._silly('_accept: exit');
        return result;
      });
    } catch (error) {
      this.logger.info(`Could not accept message for ${this.queueName}. Caller: ${caller} Error: ${error.message}`);
      this._silly('_accept: exit (error)');
      return Q();
    }
  }

  done(request) {
    this._silly('done: enter');
    if (!request || !request._message || !this.receiver) {
      this._silly('done: exit (nothing to do)');
      return Q();
    }
    // delete the message so a subsequent abandon or done does not retry the ack/nak
    this._incrementMetric('done');
    const message = request._message;
    delete request._message;
    return this._accept(message, 'done').then(result => {
      this._silly(`done: exit (ACKed: ${request.type} ${request.url})`);
      return result;
    });
  }

  /**
   * Don't give up on the given request but also don't immediately try it again -- defer try
   */
  defer(request) {
    this._silly('defer: enter');
    const message = request._message;
    if (!message) {
      this._silly('defer: exit (nothing to do)');
      return;
    }
    this._incrementMetric('defer');
    // TODO allow the caller to pass in the wake up time.
    message._deferUntil = moment().add(500, 'ms');
    this.messages.push(message);
    delete request._message;
    this._silly(`defer: exit (DEFERed: ${request.type} ${request.url})`);
  }

  abandon(request) {
    this._silly('abandon: enter');
    if (!request || !request._message || !this.receiver) {
      this._silly('abandon: nothing to do');
      return Q();
    }
    // delete the message so a subsequent abandon or done does not retry the ack/nak
    this._incrementMetric('abandon');
    const message = request._message;
    delete request._message;

    return this._release(message, 'abandon').then(result => {
      this._silly(`abandon: exit (NAKed: ${request.type} ${request.url})`);
      return result;
    });
  }

  flush() {
    this._silly('flush: enter');
    if (!this.manager) {
      this._silly('flush: exit (no manager)');
      return Q();
    }
    return Q
      .try(this.unsubscribe.bind(this))
      .then(this.manager.flushQueue.bind(this.manager, this.queueName))
      .then(this.subscribe.bind(this))
      .then(() => {
        this._silly('flush: exit');
        return this;
      });
  }

  getInfo() {
    if (!this.manager) {
      return Q(null);
    }
    return this.manager.getInfo(this.queueName).then(info => {
      if (!info) {
        return null;
      }
      info.metricsName = `${this.options.queueName}:${this.name}`;
      return info;
    });
  }

  getName() {
    return this.name;
  }

  _reconfigure(current, changes) {
    if (changes.some(patch => patch.path === '/credit') && this.currentAmqpCredit !== this.options.credit) {
      this.logger.info(`Reconfiguring AMQP 1.0 credit from ${this.currentAmqpCredit} to ${this.options.credit} for ${this.getName()}`);
      this.receiver.addCredits(this.options.credit - this.currentAmqpCredit);
      this.currentAmqpCredit = this.options.credit;
    }
    return Q();
  }

  _incrementMetric(operation) {
    const metrics = this.logger.metrics;
    if (metrics && metrics[this.name] && metrics[this.name][operation]) {
      metrics[this.name][operation].incr();
    }
  }

  _silly(message) {
    if (this.logger) {
      this.logger.silly(message);
    }
    this.debug(message);
  }

  _logReceiverSenderError(err, type) {
    if (err.condition === 'amqp:link:detach-forced' || err.condition === 'amqp:connection:forced') {
      this.logger.info(`${this.queueName} - ${type} timeout: ${err.condition}`);
    } else {
      this.logger.error(err, `${this.queueName} - ${type} error`);
    }
  }
}

module.exports = Amqp10Queue;
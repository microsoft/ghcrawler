// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Amqp10Queue = require('../../providers/queuing/amqp10Queue');
const config = require('painless-config');
const expect = require('chai').expect;
const CrawlerFactory = require('../../lib/crawlerFactory');
const Q = require('q');
const Request = require('ghcrawler').request;

const url = config.get('CRAWLER_AMQP10_URL'); // URL should be: amqps://<keyName>:<key>@<host>
const name = 'test';
const formatter = message => {
  Request.adopt(message);
  return message;
};
const options = {
  logger: CrawlerFactory.createLogger(true, 'silly'),
  queueName: 'ghcrawler',
  credit: 2,
  _config: { on: () => { } }
};

describe('AMQP 1.0 Integration', () => {

  before(() => {
    if (!url) {
      throw new Error('CRAWLER_AMQP10_URL not configured.');
    }
    return drainTestQueue(100);
  });

  it('Should pop no message if the queue is empty', () => {
    const amqpQueue = new Amqp10Queue(url, name, formatter, options);
    return amqpQueue.subscribe().then(() => {
      return amqpQueue.pop().then(message => {
        expect(message).to.be.null;
        return amqpQueue.unsubscribe();
      });
    });
  });

  it('Should push, pop and ack a message', (done) => {
    const amqpQueue = new Amqp10Queue(url, name, formatter, options);
    amqpQueue.subscribe().then(() => {
      let msg = new Request('user', 'http://test.com/users/user1');
      amqpQueue.push(msg).then(() => {
        setTimeout(() => {
          amqpQueue.pop().then(message => {
            expect(message).to.exist;
            expect(message instanceof Request).to.be.true;
            amqpQueue.done(message).then(() => {
              amqpQueue.unsubscribe().then(done());
            });
          });
        }, 500);
      });
    });
  });

  it('Should push, pop and ack a message, then pop no message from the empty queue', (done) => {
    const amqpQueue = new Amqp10Queue(url, name, formatter, options);
    amqpQueue.subscribe().then(() => {
      let msg = new Request('user', 'http://test.com/users/user2');
      amqpQueue.push(msg).then(() => {
        setTimeout(() => {
          amqpQueue.pop().then(message => {
            expect(message).to.exist;
            expect(message instanceof Request).to.be.true;
            amqpQueue.done(message).then(() => {
              amqpQueue.pop().then(emptyMessage => {
                expect(emptyMessage).to.be.null;
                amqpQueue.unsubscribe().then(done());
              });
            });
          });
        }, 500);
      });
    });
  });

  it('Should push, pop, abandon, pop and ack a message', (done) => {
    const amqpQueue = new Amqp10Queue(url, name, formatter, options);
    amqpQueue.subscribe().then(() => {
      let msg = new Request('user', 'http://test.com/users/user3');
      amqpQueue.push(msg).then(() => {
        setTimeout(() => {
          amqpQueue.pop().then(message => {
            expect(message).to.exist;
            expect(message instanceof Request).to.be.true;
            amqpQueue.abandon(message).then(() => {
              setTimeout(() => {
                amqpQueue.pop().then(abandonedMessage => {
                  expect(abandonedMessage).to.exist;
                  expect(abandonedMessage instanceof Request).to.be.true;
                  amqpQueue.done(abandonedMessage).then(() => {
                    amqpQueue.unsubscribe().then(done());
                  });
                });
              }, 500);
            });
          });
        }, 500);
      });
    });
  });

  it('Should subscribe, unsubscribe, subscribe, push, pop, ack.', (done) => {
    const amqpQueue = new Amqp10Queue(url, name, formatter, options);
    const msg = new Request('user', 'http://test.com/users/user4');
    amqpQueue.subscribe().delay(200).then(() => {
      amqpQueue.unsubscribe().then(() => {
        amqpQueue.subscribe().delay(200).then(() => {
          amqpQueue.push(msg).delay(1000).then(() => {
            amqpQueue.pop().then(message => {
              expect(message).to.be.not.null;
              amqpQueue.done(message).then(() => {
                amqpQueue.unsubscribe().then(done());
              });
            });
          });
        });
      });
    });
  });

  it('Should push without connecting, fail, try unsubscribibg', (done) => {
    const amqpQueue = new Amqp10Queue(url, name, formatter, options);
    const msg = new Request('user', 'http://test.com/users/user4');
    amqpQueue.push(msg).then(message => { }, reason => {
      expect(reason).to.be.not.null;
      amqpQueue.unsubscribe().then(done());
    });
  });

  it('Should push pop and ack 10 messages when initial credit is 10', () => {
    const pushPromises = [];
    const popPromises = [];
    options.credit = 10;
    const amqpQueue = new Amqp10Queue(url, name, formatter, options);
    return amqpQueue.subscribe().delay(2000).then(() => {
      for (let i = 1; i <= 10; i++) {
        let msg = new Request('user', 'http://test.com/users/user' + i);
        pushPromises.push(amqpQueue.push(msg));
      }
      return Q.all(pushPromises).then(() => {
        for (let i = 1; i <= 10; i++) {
          popPromises.push(amqpQueue.pop().then(message => {
            expect(message).to.exist;
            expect(message instanceof Request).to.be.true;
            return amqpQueue.done(message);
          }));
        }
        return Q.all(popPromises).then(() => {
          return amqpQueue.unsubscribe();
        });
      });
    });
  });
});

function drainTestQueue(numOfMessages) {
  console.log('Drain the testing queue.');
  const deferred = Q.defer();
  const popPromises = [];
  options.credit = numOfMessages;
  const amqpQueue = new Amqp10Queue(url, name, formatter, options);
  amqpQueue.subscribe().then(() => {
    setTimeout(() => { // Wait for messages to be read.
      for (let i = 0; i < numOfMessages; i++) {
        popPromises.push(amqpQueue.pop().then(message => {
          amqpQueue.done(message);
        }));
      }
      Q.all(popPromises).then(() => {
        amqpQueue.unsubscribe().then(deferred.resolve());
      });
    }, 2000);
  });
  return deferred.promise;
}
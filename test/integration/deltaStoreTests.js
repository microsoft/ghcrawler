// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const expect = require('chai').expect;
const CrawlerFactory = require('../../lib/crawlerFactory');
const Q = require('q');
const qlimit = require('qlimit');
const sinon = require('sinon');

let deltaStore;

describe('Delta Store Integration', function () {
  this.timeout(5000);

  before(() => {
    const baseStore = {
      connect: () => logAndResolve('connect'),
      upsert: () => logAndResolve('upsert'),
      get: () => logAndResolve('get'),
      etag: () => logAndResolve('etag'),
      close: () => logAndResolve('close')
    };
    deltaStore = CrawlerFactory.createDeltaStore(baseStore);
  });

  it('Should connect, get, etag and close', () => {
    return Q.all([
      deltaStore.connect(),
      deltaStore.get('test', 'test'),
      deltaStore.etag('test', 'test'),
      deltaStore.close()
    ]);
  });

  it('Should connect and upsert twice', () => {
    return deltaStore.connect()
      .then(() => { return deltaStore.upsert({ test: process.hrtime().join(' ') }); })
      .then(() => { return deltaStore.upsert({ test: process.hrtime().join(' ') }); });
  });

  it('Should connect and upsert many times', () => {
    sinon.spy(deltaStore, '_azureAppend');
    const document = { abc: 1 };
    const docs = [];
    for (let i = 0; i < 50; i++) {
      docs.push(document);
    }
    let counter = 0;
    return deltaStore.connect().then(() => {
      return Q.all(docs.map(qlimit(10)(doc => {
        console.log(++counter);
        return deltaStore.upsert(doc);
      })));
    }).then(() => {
      expect(deltaStore._azureAppend.callCount).to.be.equal(50);
    });
  });
});

function logAndResolve(name) {
  console.log(`Called baseStore.${name}()`);
  return Q();
}

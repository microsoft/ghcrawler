// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const expect = require('chai').expect;
const DeltaStore = require('../../providers/storage/deltaStore');
const Q = require('q');
const sinon = require('sinon');

let baseStore;

describe('Logging Store', () => {
  beforeEach(() => {
    baseStore = {
      connect: sinon.spy(() => Q()),
      upsert: sinon.spy(() => Q()),
      get: sinon.spy(() => Q()),
      etag: sinon.spy(() => Q()),
      close: sinon.spy(() => Q())
    };
  });

  afterEach(() => {
    baseStore.connect.reset();
    baseStore.upsert.reset();
    baseStore.get.reset();
    baseStore.etag.reset();
    baseStore.close.reset();
  });

  it('Should connect, get, etag and close', () => {
    let blobService = {
      createContainerIfNotExists: sinon.spy((name, cb) => { cb(null); })
    };
    let deltaStore = new DeltaStore(baseStore, blobService, 'test');
    return Q.all([
      deltaStore.connect(),
      deltaStore.get('test', 'test'),
      deltaStore.etag('test', 'test'),
      deltaStore.close()
    ]).then(() => {
      expect(blobService.createContainerIfNotExists.callCount).to.be.equal(1);
      expect(baseStore.connect.callCount).to.be.equal(1);
      expect(baseStore.upsert.callCount).to.be.equal(0);
      expect(baseStore.get.callCount).to.be.equal(1);
      expect(baseStore.etag.callCount).to.be.equal(1);
      expect(baseStore.close.callCount).to.be.equal(1);
    });
  });

  it('Should upsert ten times', () => {
    let blobService = {
      createAppendBlobFromText: sinon.spy((name, blobName, text, cb) => { cb(); }),
      appendBlockFromText: sinon.spy((name, blobName, text, cb) => { cb(); })
    };
    deltaStore = new DeltaStore(baseStore, blobService, 'test');
    const promises = [];
    for (let i = 0; i < 10; i++) {
      promises.push(deltaStore.upsert({ test: true }));
    }
    return Q.all(promises).then(() => {
      expect(blobService.createAppendBlobFromText.callCount).to.be.equal(0);
      expect(blobService.appendBlockFromText.callCount).to.be.equal(10);
      expect(baseStore.upsert.callCount).to.be.equal(10);
      expect(deltaStore.blobSequenceNumber).to.be.equal(1);
      expect(deltaStore.name).to.be.equal('test');
    });
  });

  it('Should create blob if not exists', () => {
    const appendResponses = [{ statusCode: 404 }, { statusCode: 404 }, null];
    let blobService = {
      createAppendBlobFromText: sinon.spy((name, blobName, text, cb) => { cb(); }),
      appendBlockFromText: sinon.spy((name, blobName, text, cb) => { cb(appendResponses.shift()); })
    };
    deltaStore = new DeltaStore(baseStore, blobService, 'test');
    return deltaStore.upsert({ test: true }).then(() => {
      expect(blobService.createAppendBlobFromText.callCount).to.be.equal(1);
      expect(blobService.appendBlockFromText.callCount).to.be.above(1);
      expect(baseStore.upsert.callCount).to.be.equal(1);
      expect(deltaStore.blobSequenceNumber).to.be.equal(1);
    });
  });

  it('Should increment blob sequence number', () => {
    const appendResponses = [{ statusCode: 409 }, { statusCode: 409 }, { statusCode: 404 }, null];
    let blobService = {
      createAppendBlobFromText: sinon.spy((name, blobName, text, cb) => { cb(); }),
      appendBlockFromText: sinon.spy((name, blobName, text, cb) => { cb(appendResponses.shift()); })
    };
    deltaStore = new DeltaStore(baseStore, blobService, 'test');
    return deltaStore.upsert({ test: true }).then(() => {
      expect(blobService.createAppendBlobFromText.callCount).to.be.equal(1);
      expect(blobService.appendBlockFromText.callCount).to.be.above(1);
      expect(baseStore.upsert.callCount).to.be.equal(1);
      expect(deltaStore.blobSequenceNumber).to.be.equal(2);
    });
  });
});
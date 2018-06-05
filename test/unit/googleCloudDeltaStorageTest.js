// Copyright (c) Google LLC. All rights reserved.
// Licensed under the MIT License.

const expect = require('chai').expect;
const GoogleCloudDeltaStorage = require('../../providers/storage/googleCloudDeltaStorage');
const Q = require('q');
const sinon = require('sinon');
const Storage = require('@google-cloud/storage');

let baseStore;

describe('Google Cloud Delta Storage', () => {
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

  it('Should connect', () => {
    let bucketCreated = false;
    let bucket = {
      exists: sinon.spy(() => Q([bucketCreated])),
      create: sinon.spy(() => {
        bucketCreated = true;
        return Q();
      }),
    };
    let storage = {
      bucket: sinon.spy(() => bucket)
    };
    let deltaStore = new GoogleCloudDeltaStorage(
      baseStore,
      storage,
      'test-bucketName',
    );

    return deltaStore.connect()
      .then(() => {
        expect(baseStore.connect.callCount).to.be.equal(1);
        expect(bucket.exists.callCount).to.be.equal(1);
        expect(bucket.create.callCount).to.be.equal(1);
      })

      .then(() => {
        baseStore.connect.reset();
        bucket.exists.reset();
        bucket.create.reset();
        return deltaStore.connect();
      })
      .then(() => {
        expect(baseStore.connect.callCount).to.be.equal(1);
        expect(bucket.exists.callCount).to.be.equal(1);
        expect(bucket.create.callCount).to.be.equal(0);
      });
  });

  it('Should get, etag, close', () => {
    let bucket = {
      exists: sinon.spy(() => Q([true])),
    };
    let storage = {
      bucket: sinon.spy(() => bucket)
    };
    let deltaStore = new GoogleCloudDeltaStorage(
      baseStore,
      storage,
      'test-bucketName',
    );
    return Q.all([
        deltaStore.get('test-type', 'test-key'),
        deltaStore.etag('test-type', 'test-key'),
        deltaStore.close(),
      ])
      .then(() => {
        expect(baseStore.get.callCount).to.be.equal(1);
        expect(baseStore.get.args[0]).to.deep.equal(['test-type', 'test-key']);

        expect(baseStore.etag.callCount).to.be.equal(1);
        expect(baseStore.etag.args[0]).to.deep.equal(['test-type', 'test-key']);

        expect(baseStore.close.callCount).to.be.equal(1);
      });
  });

  it('Should upsert multiple times', () => {
    let file = {
      save: sinon.spy(() => Q()),
    };
    let bucket = {
      exists: sinon.spy(() => Q([true])),
      file: sinon.spy(() => file),
    };
    let storage = {
      bucket: sinon.spy(() => bucket)
    };
    let deltaStore = new GoogleCloudDeltaStorage(
      baseStore,
      storage,
      'test-bucketName',
    );
    upsertCount = 5;
    return deltaStore.connect().then(() => {
      const promises = [];
      for (let i = 0; i < upsertCount; i++) {
        promises.push(deltaStore.upsert({ test: `text-${i}` }));
      }
      return Q.all(promises)
    }).then(() => {
      expect(file.save.callCount).to.be.equal(upsertCount);
      expect(baseStore.upsert.callCount).to.be.equal(upsertCount);
      expect(file.save.args).to.deep.equal([
        [JSON.stringify({ test: `text-0` }), { contentType: 'application/json' }],
        [JSON.stringify({ test: `text-1` }), { contentType: 'application/json' }],
        [JSON.stringify({ test: `text-2` }), { contentType: 'application/json' }],
        [JSON.stringify({ test: `text-3` }), { contentType: 'application/json' }],
        [JSON.stringify({ test: `text-4` }), { contentType: 'application/json' }],
      ]);
    });
  });
});

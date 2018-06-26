// Copyright (c) Google LLC. All rights reserved.
// Licensed under the MIT License.

const crypto = require('crypto');
const moment = require('moment');
const uuid = require('node-uuid');
const Q = require('q');

class GoogleCloudDeltaStorage {
  constructor(baseStore, storage, bucketName) {
    this.baseStore = baseStore;

    // These values are used to keep delta files managed and unique
    this.uniqueBlobId = uuid.v4(); // Avoid clashes in multiple processes environment

    this.bucketName = bucketName;
    this.bucket = null;
    this.storage = storage;
  }

  connect() {
    return this.baseStore.connect().then(() => {
      this.bucket = this.storage.bucket(this.bucketName);
      return this.bucket.exists().then((data) => {
        if (!data[0]) {
          return this.bucket.create();
        }
      });
    });
  }

  upsert(document) {
    return this.baseStore.upsert(document).then(() => {
      const text = JSON.stringify(document);
      return this._stashDelta(text);
    });
  }

  get(type, key) {
    return this.baseStore.get(type, key);
  }

  etag(type, key) {
    return this.baseStore.etag(type, key);
  }

  list(type) {
    return this.baseStore.list(type);
  }

  count(type) {
    return this.baseStore.count(type);
  }

  close() {
    return this.baseStore.close();
  }

  _stashDelta(text) {
    const fileName = this._getFilename(text);
    const file = this.bucket.file(fileName);
    return file.save(text, {
      contentType: 'application/json',
    });
  }

  _getFilename(text) {
    const now = moment.utc();
    const year = now.format('YYYY');
    const month = now.format('MM');
    const day = now.format('DD');
    const hour = now.format('HH');
    const formattedDate = now.format('YYYY_MM_DD_HH');

    const fileHash = crypto.createHash('md5').update(text).digest('hex');
    return `v1/${year}/${month}/${day}/${hour}/${formattedDate}_${fileHash}_${this.uniqueBlobId}.json`;
  }
}

module.exports = GoogleCloudDeltaStorage;

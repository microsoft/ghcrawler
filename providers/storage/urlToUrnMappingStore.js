// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Q = require('q');

class UrltoUrnMappingStore {
  constructor(baseStore, redisClient, name, options) {
    this.baseStore = baseStore;
    this.redisClient = redisClient;
    this.name = name;
    this.options = options;
  }

  connect() {
    return this.baseStore.connect();
  }

  upsert(document) {
    return this.baseStore.upsert(document).then(blobName => {
      const url = document._metadata.url;
      const urn = document._metadata.links.self.href;
      const deferred = Q.defer();
      this.redisClient.hmset(this.name, [urn, blobName, url, blobName], this._callbackToPromise(deferred));
      return deferred.promise;
    });
  }

  get(type, key) {
    return this._getBlobNameForKey(key).then(blobName => {
      if (!blobName) {
        throw new Error(`Document not found at ${key}`);
      }
      return this.baseStore.get(type, blobName);
    });
  }

  etag(type, key) {
    return this._getBlobNameForKey(key).then(blobName => {
      return blobName ? this.baseStore.etag(type, blobName) : null;
    });
  }

  list(type) {
    return this.baseStore.list(type);
  }

  delete(type, key) {
    return this.baseStore.delete(type, key).catch(() => {
      return this.get(type, key).then(document => {
        const anotherKey = key === document._metadata.url ? document._metadata.links.self.href : document._metadata.url;
        return this.baseStore.delete(type, anotherKey);
      });
    });
  }

  count(type) {
    return this.baseStore.count(type);
  }

  close() {
    return this.baseStore.close();
  }

  _getBlobNameForKey(key) {
    const deferred = Q.defer();
    this.redisClient.hget(this.name, key, this._callbackToPromise(deferred));
    return deferred.promise;
  }

  _callbackToPromise(deferred) {
    return (error, value) => {
      return error ? deferred.reject(error) : deferred.resolve(value);
    };
  }
}

module.exports = UrltoUrnMappingStore;
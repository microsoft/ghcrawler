// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Q = require('q');

class UrlToUrnMappingStore {
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

  get(type, url) {
    return this._getBlobNameForUrl(url).then(urn => {
      if (!urn) {
        throw new Error(`Document not found at ${url}`);
      }
      return this.baseStore.get(type, urn);
    });
  }

  etag(type, url) {
    return this._getBlobNameForUrl(url).then(urn => {
      return urn ? this.baseStore.etag(type, urn) : null;
    });
  }

  list(type) {
    return this.baseStore.list(type);
  }

  delete(type, url) {
    return this.baseStore.delete(type, url);
  }

  count(type) {
    return this.baseStore.count(type);
  }

  close() {
    return this.baseStore.close();
  }

  _getBlobNameForUrl(url) {
    const deferred = Q.defer();
    this.redisClient.hget(this.name, url, this._callbackToPromise(deferred));
    return deferred.promise;
  }

  _callbackToPromise(deferred) {
    return (error, value) => {
      return error ? deferred.reject(error) : deferred.resolve(value);
    };
  }
}

module.exports = UrlToUrnMappingStore;
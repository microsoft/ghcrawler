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

  get(type, url) {
    return this._getUrnForUrl(url).then(urn => {
      if (!urn) {
        throw new Error(`Document not found at ${url}`);
      }
      return this.baseStore.get(type, urn);
    });
  }

  etag(type, url) {
    return this._getUrnForUrl(url).then(urn => {
      return urn ? this.baseStore.etag(type, urn) : null;
    });
  }

  list(pattern) {
    return this.baseStore.list(pattern);
  }

  delete(type, url) {
    return this.baseStore.delete(type, url);
  }

  count(pattern) {
    return this.baseStore.count(pattern);
  }

  close() {
    return this.baseStore.close();
  }

  _getUrnForUrl(url) {
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

module.exports = UrltoUrnMappingStore;
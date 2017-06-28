// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Q = require('q');

class AzureTableMappingStore {
  constructor(baseStore, tableService, name, options) {
    this.baseStore = baseStore;
    this.service = tableService;
    this.name = name;
    this.options = options;
  }

  connect() {
    return this.baseStore.connect().then(() => {
      return this._createTable(this.name);
    });
  }

  upsert(document) {
    return this.baseStore.upsert(document).then(blobName => {
      const url = document._metadata.url;
      const urn = document._metadata.links.self.href;
      const type = document._metadata.type;
      const deferred = Q.defer();
      const urlEntity = { PartitionKey: { '_': `url:${type}` }, RowKey: { '_': encodeURIComponent(url) }, blobName: { '_': blobName } };
      const urnEntity = { PartitionKey: { '_': `urn:${type}` }, RowKey: { '_': encodeURIComponent(urn) }, blobName: { '_': blobName } };
      this.service.insertOrReplaceEntity(this.name, urlEntity, (error) => {
        if (error) {
          return deferred.reject(error);
        }
        this.service.insertOrReplaceEntity(this.name, urnEntity, this._callbackToPromise(deferred));
      });

      return deferred.promise;
    });
  }

  get(type, key) {
    return this._getBlobNameForKey(type, key).then(blobName => {
      if (!blobName) {
        throw new Error(`Document not found at ${key}`);
      }
      return this.baseStore.get(type, blobName);
    });
  }

  etag(type, key) {
    return this._getBlobNameForKey(type, key).then(blobName => {
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

  _createTable(name) {
    const createTableIfNotExists = Q.nbind(this.service.createTableIfNotExists, this.service);
    return createTableIfNotExists(name);
  }

  _getBlobNameForKey(type, key) {
    const deferred = Q.defer();
    const prefix = key && key.startsWith('urn:') ? 'urn' : 'url';
    this.service.retrieveEntity(this.name, `${prefix}:${type}`, encodeURIComponent(key), (error, result) => {
      if (!error) {
        return deferred.resolve(result.blobName._);
      }
      if (error && error.code === 'ResourceNotFound') {
        return deferred.resolve(null);
      }
      deferred.reject(error);
    });
    return deferred.promise;
  }

  _callbackToPromise(deferred) {
    return (error, value) => {
      return error ? deferred.reject(error) : deferred.resolve(value);
    };
  }
}

module.exports = AzureTableMappingStore;
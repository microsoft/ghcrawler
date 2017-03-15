// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Q = require('q');

class AzureTableMappingStore {
  constructor(baseStore, tableService, redisClient, name, options) { //TODO: remove "redisClient" after Redis to Azure table data migration
    this.baseStore = baseStore;
    this.service = tableService;
    this.redisClient = redisClient; //TODO: remove after Redis to Azure table data migration
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
      this.redisClient.hmset(this.name, [urn, blobName, url, blobName], (error) => { //TODO: remove after the data migration except for "this.service..."
        if (error) {
          return deferred.reject(error);
        }
        this.service.insertOrReplaceEntity(this.name, urlEntity, (error) => {
          if (error) {
            return deferred.reject(error);
          }
          this.service.insertOrReplaceEntity(this.name, urnEntity, this._callbackToPromise(deferred));
        });
      });
      return deferred.promise;
    });
  }

  get(type, url) {
    return this._getBlobNameForUrl(type, url).then(blobName => {
      if (!blobName) {
        throw new Error(`Document not found at ${url}`);
      }
      return this.baseStore.get(type, blobName);
    });
  }

  etag(type, url) {
    return this._getBlobNameForUrl(type, url).then(blobName => {
      return blobName ? this.baseStore.etag(type, blobName) : null;
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

  _createTable(name) {
    const createTableIfNotExists = Q.nbind(this.service.createTableIfNotExists, this.service);
    return createTableIfNotExists(name);
  }

  _getBlobNameForUrl(type, url) {
    const deferred = Q.defer();
    this.redisClient.hget(this.name, url, this._callbackToPromise(deferred)); // TODO: remove this line and uncomment below after the data migration
    // this.service.retrieveEntity(this.name, `url:${type}`, encodeURIComponent(url), (error, result) => {
    //   if (!error) {
    //     return deferred.resolve(result.blobName._);
    //   }
    //   if (error && error.code === 'ResourceNotFound') {
    //     return deferred.resolve(null);
    //   }
    //   deferred.reject(error);
    // });
    return deferred.promise;
  }

  _callbackToPromise(deferred) {
    return (error, value) => {
      return error ? deferred.reject(error) : deferred.resolve(value);
    };
  }
}

module.exports = AzureTableMappingStore;
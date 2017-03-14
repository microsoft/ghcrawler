// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const azure = require('azure-storage');
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
      const deferred = Q.defer();
      const urlBatch = { PartitionKey: { '_': this.name }, RowKey: { '_': encodeURIComponent(url) }, blobName: { '_': blobName } };
      const urnBatch = { PartitionKey: { '_': this.name }, RowKey: { '_': encodeURIComponent(urn) }, blobName: { '_': blobName } };
      const batch = new azure.TableBatch();
      batch.insertOrReplaceEntity(urlBatch);
      batch.insertOrReplaceEntity(urnBatch);
      this.service.executeBatch(this.name, batch, this._callbackToPromise(deferred));
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

  _createTable(name) {
    const createTableIfNotExists = Q.nbind(this.service.createTableIfNotExists, this.service);
    return createTableIfNotExists(name);
  }

  _getBlobNameForUrl(url) {
    const deferred = Q.defer();
    this.service.retrieveEntity(this.name, this.name, encodeURIComponent(url), (error, result) => {
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
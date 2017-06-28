// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Q = require('q');

class InmemoryDocStore {
  constructor() {
    this.collections = {};
  }

  connect() {
    return Q(null);
  }

  upsert(document) {
    const type = document._metadata.type;
    const url = document._metadata.url;
    const urn = document._metadata.links.self.href;
    let collection = this.collections[type];
    if (!collection) {
      collection = {};
      this.collections[type] = collection;
    }
    collection[url] = document;
    collection[urn] = document;
    return Q(document);
  }

  get(type, key) {
    const collection = this.collections[type];
    if (!collection) {
      return Q.reject();
    }
    return collection[key] ? Q(collection[key]) : Q.reject();
  }

  etag(type, key) {
    const collection = this.collections[type];
    if (!collection) {
      return Q(null);
    }
    let result = collection[key];
    result = result ? result._metadata.etag : null;
    return Q(result);
  }

  list(type) {
    let collection = this.collections[type];
    if (!collection) {
      collection = {};
    }
    return Q(Object.keys(collection).filter(key => {
      return key.startsWith('urn:') ? true : false;
    }).map(key => {
      const metadata = collection[key]._metadata;
      return {
        version: metadata.version,
        etag: metadata.etag,
        type: metadata.type,
        url: metadata.url,
        urn: metadata.links.self.href,
        fetchedAt: metadata.fetchedAt,
        processedAt: metadata.processedAt,
        extra: metadata.extra
      };
    }));
  }

  delete(type, key) {
    const collection = this.collections[type];
    if (!collection) {
      return Q(null);
    }
    const document = collection[key];
    if (document) {
      const anotherKey = key === document._metadata.url ? document._metadata.links.self.href : document._metadata.url;
      delete collection[anotherKey];
    }
    delete collection[key];
    return Q(true);
  }

  count(type) {
    return this.list(type).then(results => { return results.length });
  }

  close() {
    this.collections = {};
  }
}

module.exports = InmemoryDocStore;
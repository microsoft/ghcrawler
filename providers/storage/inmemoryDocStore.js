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
    const selfHref = document._metadata.links.self.href;
    const type = document._metadata.type;
    let collection = this.collections[type];
    if (!collection) {
      collection = {};
      this.collections[type] = collection;
    }
    collection[selfHref] = document;
    return Q(document);
  }

  get(type, url) {
    // TODO interesting question as to what a mongo store would do if the doc does not exist.
    const collection = this.collections[type];
    if (!collection) {
      return Q.reject();
    }
    return collection[url] ? Q(collection[url]) : Q.reject();
  }

  etag(type, url) {
    // TODO interesting question as to what a mongo store would do if the doc does not exist.
    const collection = this.collections[type];
    if (!collection) {
      return Q(null);
    }
    let result = collection[url];
    result = result ? result._metadata.etag : null;
    return Q(result);
  }

  list(type) {
    return Q(this.collections[type].map(doc => {
      const metadata = doc._metadata;
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

  delete(type, url) {
    const collection = this.collections[type];
    if (!collection) {
      return Q(null);
    }
    delete collection[url];
    return Q(true);
  }

  count(type) {
    return this.list(type).then(results => { return results.length });
  }

  close() {
    content = {};
  }
}

module.exports = InmemoryDocStore;
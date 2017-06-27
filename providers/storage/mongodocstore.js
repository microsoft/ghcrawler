// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const memoryCache = require('memory-cache');
const Mongo = require('mongodb');
const promiseRetry = require('promise-retry');
const Q = require('q');

class MongoDocStore {
  constructor(url, options) {
    this.url = url;
    this.options = options;
    this.client = Mongo.MongoClient;
  }

  connect() {
    return promiseRetry((retry, number) => {
      return this.client.connect(this.url).then(db => {
        this.db = db;
      })
        .catch(retry);
    });
  }

  upsert(document) {
    const selfHref = document._metadata.links.self.href;
    const collection = this.db.collection(document._metadata.type);
    return collection.updateOne({ '_metadata.links.self.href': selfHref }, document, { upsert: true }).then(result => {
      memoryCache.put(document._metadata.url, { etag: document._metadata.etag, document: document }, this.options.ttl);
      return result;
    });
  }

  get(type, key) {
    const cached = memoryCache.get(key);
    if (cached) {
      return Q(cached.document);
    }
    return this.db.collection(type).findOne({ '$or': [{ '_metadata.url': key }, { '_metadata.links.self.href': key }] }).then(value => {
      if (value) {
        const url = value._metadata.url;
        memoryCache.put(url, { etag: value._metadata.etag, document: value }, this.options.ttl);
        return value;
      }
      return null;
    });
  }

  etag(type, key) {
    const cached = memoryCache.get(key);
    if (cached) {
      return Q(cached.etag);
    }
    const filter = key && key.startsWith('urn:') ? '_metadata.links.self.href' : '_metadata.url';
    return this.db.collection(type).findOne({ filter: url }).then(value => {
      if (value) {
        const url = value._metadata.url;
        memoryCache.put(url, { etag: value._metadata.etag, document: value }, this.options.ttl);
        return value._metadata.etag;
      }
      return null;
    });
  }

  list(type) {
    return this.db.collection(type).find({}, { '_metadata': 1 }).toArray().then(docs => {
      return docs.map(doc => {
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
      })
    });
  }

  delete(type, key) {
    const filter = key && key.startsWith('urn:') ? '_metadata.links.self.href' : '_metadata.url';
    return this.db.collection(type).deleteOne({ filter: key }).then(result => {
      return result;
    });
  }

  count(type) {
    return this.db.collection(type).count()
  }

  close() {
    this.db.close();
  }
}

module.exports = MongoDocStore;
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

  get(type, url) {
    const cached = memoryCache.get(url);
    if (cached) {
      return Q(cached.document);
    }
    return this.db.collection(type).findOne({ '$or': [{ '_metadata.url': url }, { '_metadata.links.self.href': url }] }).then(value => {
      if (value) {
        memoryCache.put(url, { etag: value._metadata.etag, document: value }, this.options.ttl);
        return value;
      }
      return null;
    });
  }

  etag(type, url) {
    const cached = memoryCache.get(url);
    if (cached) {
      return Q(cached.etag);
    }
    return this.db.collection(type).findOne({ '_metadata.url': url }).then(value => {
      if (value) {
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
        Object.keys(metadata).forEach(key => {
          if (key !== key.toLowerCase()) {
            metadata[key.toLowerCase()] = metadata[key];
            delete metadata[key];
          }
        });
        metadata.urn = metadata.links.self.href;
        return metadata;
      })
    });
  }

  delete(type, url) {
    return this.db.collection(type).deleteOne({ $or: [{ '_metadata.url': url }, { '_metadata.links.self.href': url }] }).then(result => {
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
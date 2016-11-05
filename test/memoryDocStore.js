class MemoryDocStore {
  constructor() {
  }

  connect() {
    this.store = {};
  }

  upsert(document, callback) {
    const selfHref = document._metadata.links.self.href;
    this.store[selfHref] = { etag = Date.now, document: document };
    callback();
  }

  etag(url) {
    const result = this.store[url];
    return result ? result.etag : null;
  }

  close() {
    this.store = null;
  }
}
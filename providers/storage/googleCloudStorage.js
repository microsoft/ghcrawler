// Copyright (c) Google LLC. All rights reserved.
// Licensed under the MIT License.

const Storage = require('@google-cloud/storage');
const memoryCache = require('memory-cache');
const Q = require('q');

function getMetadataFromDocument(document) {
  const metadata = {
    version: document._metadata.version,
    etag: document._metadata.etag,
    type: document._metadata.type,
    url: document._metadata.url,
    urn: document._metadata.links.self.href,
    fetchedat: document._metadata.fetchedAt,
    processedat: document._metadata.processedAt
  };

  if (document._metadata.extra) {
    metadata.extra = JSON.stringify(document._metadata.extra);
  }

  return metadata;
}

function getMetadataFromCloudFile(file) {
  return file.getMetadata().then((data) => {
    // Using `.metadata` as this is the custom metadata we set.
    const blobMetadata = data[0].metadata;
    return {
      version: blobMetadata.version,
      etag: blobMetadata.etag,
      type: blobMetadata.type,
      url: blobMetadata.url,
      urn: blobMetadata.urn,
      fetchedAt: blobMetadata.fetchedat,
      processedAt: blobMetadata.processedat,
      extra: blobMetadata.extra ? JSON.parse(blobMetadata.extra) : undefined
    };
  });
}

function getFileNameFromUrl(type, url) {
  if (!(url.startsWith('http:') || url.startsWith('https:'))) {
    return url;
  }
  const parsed = URL.parse(url, true);
  return `${type}${parsed.path.toLowerCase()}.json`;
}

function getFileNameFromUrn(type, urn) {
  if (!urn.startsWith('urn:')) {
    return urn;
  }
  return `${getPathFromUrn(type, urn)}.json`;
}

function getPathFromUrn(type, urn) {
  if (!urn) {
    return '';
  }
  if (!urn.startsWith('urn:')) {
    return urn;
  }

  // String urn: from start and replace ':' with '/'
  return urn.slice(4).replace(/:/g, '/').toLowerCase();
}

class GoogleCloudStorage {
  constructor(bucketName, projectId, clientEmail, key, options) {
    this.bucketName = bucketName;
    this.bucket = null;

    this.options = options;

    // Environment variables will cause new lines to be encoded as'\\n'
    // which causes the google-cloud storage SDK to fail as it requires
    // '\n' characters.
    const parsedPrivateKey = key.replace(/\\n/g, '\n');

    this.storage = new Storage({
      projectId,
      credentials: {
        client_email: clientEmail,
        private_key: parsedPrivateKey,
      }
    });
  }

  connect() {
    this.bucket = this.storage.bucket(this.bucketName);
    return this.bucket.exists().then((data) => {
      if (!data[0]) {
        return this.bucket.create();
      }
    });
  }

  upsert(document) {
    const fileName = this._getFileNameFromDocument(document);
    const text = JSON.stringify(document);
    const metadata = getMetadataFromDocument(document);

    const options = {
      contentType: 'application/json',
      metadata: {
        // The Google Cloud SDK requires custom metadata to be defined under
        // `metadata`.
        // See https://github.com/googleapis/nodejs-storage/issues/222
        metadata,
      },
    };

    const file = this.bucket.file(fileName);
    return file.save(text, options).then(() => {
        memoryCache.put(fileName, {
          etag: document._metadata.etag,
          document: document,
        }, this.options.ttl);
      });
  }

  get(type, key) {
    const fileName = this._getFileName(type, key);
    const cached = memoryCache.get(fileName);
    if (cached) {
      return Q(cached.document);
    }

    const file = this.bucket.file(fileName);
    return file.get().then(function (fileContents) {
      return file.getMetadata().then(function(metadata) {
          const result = JSON.parse(fileContents);
          memoryCache.put(key, {
            // Custom metadata is nested under 'metadata' and we want
            // to use the etag provided by ghcrawler
            etag: metadata.metadata.etag,
            document: result
          }, this.options.ttl);
          return result;
        });
    });
  }

  etag(type, key) {
    const fileName = this._getFileName(type, key);
    const cached = memoryCache.get(fileName);
    if (cached) {
      return Q(cached.etag);
    }

    const file = this.bucket.file(fileName);
    return file.getMetadata().then(function (metadata) {
        return metadata.metadata.etag;
      })
      .catch(function () {
        return null;
      });
  }

  list(type) {
    return this.bucket.getFiles({
      autoPaginate: true,
      directory: type
    }).then(function (data) {
      return Q.all(data[0].map((file) => {
        return getMetadataFromCloudFile(file);
      }));
    });
  }

  delete(type, key) {
    const fileName = this._getFileName(type, key);
    const file = this.bucket.file(fileName);
    return file.delete().then(function () {
      return true;
    });
  }

  count(type) {
    return this.bucket.getFiles({
      autoPaginate: true,
      directory: type
    }).then(function (files) {
      return files.length;
    });
  }

  close() {
    return Q();
  }

  _getFileName(type, key) {
    if (this.options.blobKey === 'url') {
      return getFileNameFromUrl(type, key);
    }

    return getFileNameFromUrn(type, key);
  }

  _getFileNameFromDocument(document) {
    const type = document._metadata.type;
    const key = this.options.blobKey === 'url'?
      document._metadata.url : document._metadata.links.self.href;
    return this._getFileName(type, key);
  }
}

module.exports = GoogleCloudStorage;

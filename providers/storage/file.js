// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const fs = require('fs');
const path = require('path');
const mkdirp = require('mkdirp');

// TODO finish the implementation of the relevant methods

class FileStore {
  constructor(options) {
    this.options = options;
  }

  connect() {
    return Promise.resolve(null);
  }

  async upsert(document) {
    // TODO there may be a thing about accessing the data either by URL or URN.
    // This method is just using URN. Need to look at the usecases and clarify.
    const type = document._metadata.type;
    const urn = document._metadata.links.self.href;
    const filePath = this._getPath(urn);
    mkdirp.sync(path.dirname(filePath));
    return new Promise((resolve, reject) =>
      fs.writeFile(filePath, JSON.stringify(document, null, 2), error =>
        error ? reject(error) : resolve(document)));
  }

  async get(type, key) {
    const path = this._getPath(key);
    return new Promise((resolve, reject) =>
      fs.readFile(path, (error, data) =>
        error ? reject(error) : resolve(JSON.parse(data))));
  }

  _getPath(key) {
    key = key.toLowerCase()
    const realKey = key.startsWith('urn:') ? key.slice(4) : key;
    return `${this.options.location}/${realKey.replace(/:/g, '/')}.json`;
  }

  etag(type, key) {
    return this.get(type, key).then(result => result._metadata.etag);
  }

  list(type) {
    // TODO implement -- not really sure this is actually needed.
    return Promise.resolve([]);
  }

  delete(type, key) {
    const path = this._getPath(urn);
    return new Promise((resolve, reject) =>
      fs.unlink(path, error => error ? reject(error) : resolve(null)));
  }

  count(type) {
    // TODO likewise wrt list. Not sure this is needed
    return this.list(type).then(results => { return results.length });
  }

  close() {
  }
}

module.exports = options => new FileStore(options);
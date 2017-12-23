// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const request = require('request');
const fs = require('fs');

class ClearlyDefinedStore {
  constructor(options) {
    this.options = options;
  }

  connect() {
    return Promise.resolve(null);
  }

  upsert(document) {
    const uri = this._buildUrl(document);
    var options = {
      method: 'PUT',
      uri,
      headers: {
        authorization: `Basic ${this.options.token}`
      }
    };
    return new Promise((resolve, reject) => {
      fs.createReadStream(document.output).pipe(request(options, (error, response, body) => {
        if (error)
          return reject(error);
        if (response.statusCode === 201)
          return resolve(document);
        reject(new Error(`${response.statusCode} ${response.statusMessage}`))
      }));
    });
  }

  _buildUrl(document) {
    const urnSegments = document._metadata.links.self.href.split(':');
    return `${this.options.url}/${urnSegments.slice(2).join('/')}`;
  }

  get(type, key) {
    return Promise.reject();
  }

  etag(type, key) {
    return Promise.resolve(null);
  }

  delete(type, key) {
    return Promise.resolve(true);
  }

  list(type) {
    return Promise.resolve([]);
  }

  count(type) {
    return this.list(type).then(results => { return results.length });
  }

  close() {
  }
}

module.exports = options => new ClearlyDefinedStore(options);
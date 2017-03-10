// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const moment = require('moment');
const uuid = require('node-uuid');
const Q = require('q');

class DeltaStore {
  constructor(baseStore, blobService, name, options) {
    this.baseStore = baseStore;
    this.service = blobService;
    this.name = name;
    this.options = options;
    this.blobPromise = null;
    this.blobSequenceNumber = 1;
    this.uniqueBlobId = uuid.v4(); // Avoid clashes in multiple processes environment
    this.currentHour = moment.utc().format('HH');
  }

  connect() {
    return this.baseStore.connect().then(() => {
      return this._createContainer(this.name);
    });
  }

  upsert(document) {
    return this.baseStore.upsert(document).then(() => {
      const text = JSON.stringify(document) + '\n';
      return this._append(text);
    });
  }

  get(type, key) {
    return this.baseStore.get(type, key);
  }

  etag(type, key) {
    return this.baseStore.etag(type, key);
  }

  close() {
    return this.baseStore.close();
  }

  _createContainer(name) {
    const createContainerIfNotExists = Q.nbind(this.service.createContainerIfNotExists, this.service);
    return createContainerIfNotExists(name);
  }

  _append(text) {
    return this._azureAppend(this.name, this._getBlobName(), text).catch(error => {
      // If this is a non-recoverable error rethrow
      if (error.statusCode !== 404 && error.statusCode !== 409) {
        throw error;
      }
      // if a new blob is being created, wait for that to finish and then append our text
      // be sure to recurse here as the newly created blob may have been on the time unit boundary
      // and this new content should be written in the new time block.
      if (this.blobPromise) {
        return this.blobPromise.then(() => {
          return this._append(text);
        });
      }
      this.blobPromise = this._createBlob(text).finally(() => {
        this.blobPromise = null;
      });
      return this.blobPromise;
    });
  }

  _createBlob(text) {
    // Walk over the sequence of blobs until we find one that can take the text.  Create a new blob if needed.
    // First try to append to the current blob to ensure we are not overwriting something
    return this._azureAppend(this.name, this._getBlobName(), text).catch(error => {
      if (error.statusCode === 409) {
        this.blobSequenceNumber++;
        return this._createBlob(text);
      }
      if (error.statusCode === 404) {
        return this._azureCreate(this.name, this._getBlobName(), text);
      }
      throw error;
    });
  }

  _azureCreate(containerName, blobName, text) {
    return Q.nbind(this.service.createAppendBlobFromText, this.service)(containerName, blobName, text);
  }

  _azureAppend(containerName, blobName, text) {
    return Q.nbind(this.service.appendBlockFromText, this.service)(containerName, blobName, text);
  }

  _getBlobName() {
    const now = moment.utc();
    const year = now.format('YYYY');
    const month = now.format('MM');
    const day = now.format('DD');
    const hour = now.format('HH');
    const formatted = now.format('YYYY_MM_DD_HH');
    if (hour !== this.currentHour) {
      this.currentHour = hour;
      this.blobSequenceNumber = 1;
    }
    return `v1/${year}/${month}/${day}/${formatted}_${this.blobSequenceNumber}_${this.uniqueBlobId}.json`;
  }
}

module.exports = DeltaStore;

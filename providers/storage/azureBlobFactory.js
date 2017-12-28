// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const AzureStorage = require('azure-storage');
const AzureStorageDocStore = require('./storageDocStore');

module.exports = options => {
  options.logger.info(`creating azure storage store`);
  const { account, key, connection, container } = options;
  const retryOperations = new AzureStorage.ExponentialRetryPolicyFilter();
  const blobService = connection
    ? AzureStorage.createBlobService(connection).withFilter(retryOperations)
    : AzureStorage.createBlobService(account, key).withFilter(retryOperations);
  return new AzureStorageDocStore(blobService, container, options);
}

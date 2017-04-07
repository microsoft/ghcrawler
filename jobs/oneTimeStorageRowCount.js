// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/* This script is a point-in-time script that either counts a number of blobs in Azure blob storage container or
  total number of rows as well as partitions in Azure table.

  The following variables should be set:
  CRAWLER_STORAGE_ACCOUNT, CRAWLER_STORAGE_NAME, CRAWLER_STORAGE_KEY
  Command line arguments:
  1) 'blob' or table' (mandatory)
  2) 'countPartitions' (optional; only relevant to the table)
*/

const AzureStorage = require('azure-storage');
const config = require('painless-config');
const Q = require('q');

if (!process.argv[2] || !['blob', 'table'].includes(process.argv[2])) {
  console.log('First argument must be either blob or table');
  process.exit(1);
}
const isBlob = process.argv[2] === 'blob' ? true : false;
const shouldCountPartitions = process.argv[3] === 'countPartitions' ? true : false;

const jobConfig = {
  azureStorage: {
    account: config.get('CRAWLER_STORAGE_ACCOUNT'),
    key: config.get('CRAWLER_STORAGE_KEY'),
    name: config.get('CRAWLER_STORAGE_NAME')
  }
};

const stats = {
  batches: 0,
  total: 0
};
if (shouldCountPartitions) {
  stats.partitions = {
    count: 0,
    names: new Set()
  };
}

const retryOperations = new AzureStorage.ExponentialRetryPolicyFilter();
const blobService = AzureStorage.createBlobService(jobConfig.azureStorage.account, jobConfig.azureStorage.key).withFilter(retryOperations);
const tableService = AzureStorage.createTableService(jobConfig.azureStorage.account, jobConfig.azureStorage.key).withFilter(retryOperations);
let continuationToken = null;

console.time('Storage count');

if (isBlob) {
  console.log(new Date(), `Counting number of blobs in ${jobConfig.azureStorage.name} container.`);
  startBlobsProcessing();
} else {
  console.log(new Date(), `Counting numer of rows in ${jobConfig.azureStorage.name} table.`);
  if (shouldCountPartitions) {
    console.log(new Date(), 'Counting partitions.');
  }
  startTableProcessing();
}

function startBlobsProcessing() {
  return Q().then(retrieveBlobs)
    .catch(error => console.error(error))
    .then(retrieveNextBlobPage);
}

function startTableProcessing() {
  return Q().then(retrieveTableEntities)
    .catch(error => console.error(error))
    .then(retrieveNextTablePage);
}

function retrieveBlobs() {
  stats.batches++;
  const deferred = Q.defer();
  blobService.listBlobsSegmented(jobConfig.azureStorage.name, continuationToken, (error, result) => {
    if (error) {
      return Q.reject(error);
    }
    continuationToken = result.continuationToken;
    const blobsCount = result.entries.length;
    console.log(new Date(), { batch: stats.batches, count: blobsCount, token: continuationToken });
    stats.total += blobsCount;
    deferred.resolve();
  });
  return deferred.promise;
}

function retrieveTableEntities() {
  stats.batches++;
  const deferred = Q.defer();
  const query = new AzureStorage.TableQuery().select(['PartitionKey']);
  tableService.queryEntities(jobConfig.azureStorage.name, query, continuationToken, (error, result) => {
    if (error) {
      return Q.reject(error);
    }
    continuationToken = result.continuationToken;
    const rowCount = result.entries.length;
    console.log(new Date(), { batch: stats.batches, count: rowCount, token: continuationToken });
    stats.total += rowCount;
    if (shouldCountPartitions) {
      result.entries.forEach(entry => {
        stats.partitions.names.add(entry.PartitionKey._);
      });
    }
    deferred.resolve();
  });
  return deferred.promise;
}

function retrieveNextBlobPage() {
  if (continuationToken) {
    return startBlobsProcessing();
  }
  return completeRun();
}

function retrieveNextTablePage() {
  if (continuationToken) {
    return startTableProcessing();
  }
  return completeRun();
}

function completeRun() {
  if (!isBlob && shouldCountPartitions) {
    stats.partitions.count = stats.partitions.names.size;
  }
  console.log(stats);
  console.timeEnd('Storage count');
  return Q();
}
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/* This script is a point-in-time script that populates any missing URL and URN to blob name mappings in Azure table
  based on JSON documents stored in Azure blob container.

  The following variables should be set:
  CRAWLER_STORAGE_ACCOUNT, CRAWLER_STORAGE_NAME, CRAWLER_STORAGE_KEY
  Optional argument: continuation token value.
  Example: node jobs/oneTimePopulateTableMapping.js '{"nextMarker":"VALUE","targetLocation":0}'
*/

const AzureStorage = require('azure-storage');
const config = require('painless-config');
const Q = require('q');
const qlimit = require('qlimit');


const jobConfig = {
  azureStorage: {
    account: config.get('CRAWLER_STORAGE_ACCOUNT'),
    key: config.get('CRAWLER_STORAGE_KEY'),
    name: config.get('CRAWLER_STORAGE_NAME')
  }
};

const stats = {
  batches: 0,
  total: 0,
  url: {
    added: 0,
    skipped: 0,
    skippedReason: {
      alreadyExists: 0,
      errors: 0
    }
  },
  urn: {
    added: 0,
    skipped: 0,
    skippedReason: {
      alreadyExists: 0,
      errors: 0
    }
  }
};
const limit = qlimit(20);
const retryOperations = new AzureStorage.ExponentialRetryPolicyFilter();
const blobService = AzureStorage.createBlobService(jobConfig.azureStorage.account, jobConfig.azureStorage.key).withFilter(retryOperations);
const tableService = AzureStorage.createTableService(jobConfig.azureStorage.account, jobConfig.azureStorage.key).withFilter(retryOperations);
let continuationToken = JSON.parse(process.argv[2]) || null;

console.time('Populate table mapping');
console.log(new Date(), `Populating ${jobConfig.azureStorage.name} table.`);

start();

function start() {
  return Q().then(retrieveBlobNames)
    .then(processBatch)
    .catch(error => console.error(error))
    .then(startNext);
}

function retrieveBlobNames() {
  stats.batches++;
  console.time(`Processed batch ${stats.batches}`);
  const deferred = Q.defer();
  blobService.listBlobsSegmented(jobConfig.azureStorage.name, continuationToken, (error, result) => {
    if (error) {
      return Q.reject(error);
    }
    continuationToken = result.continuationToken;
    const blobNames = result.entries.map(entry => entry.name);
    console.log(new Date(), `Retrieved ${blobNames.length} blob names.`);
    deferred.resolve(blobNames);
  });
  return deferred.promise;
}

function processBatch(blobNames) {
  return Q.all(blobNames.map(limit(blobName => {
    stats.total++;
    return getBlobData(blobName).then(storeEntity);
  }))).then(() => {
    console.timeEnd(`Processed batch ${stats.batches}`);
    return Q();
  });
}

function getBlobData(blobName) {
  const deferred = Q.defer();
  blobService.getBlobToText(jobConfig.azureStorage.name, blobName, (error, result) => {
    if (error) {
      return Q.reject(error);
    }
    const document = JSON.parse(result);
    const blobData = {
      name: blobName,
      url: document._metadata.url,
      urn: document._metadata.links.self.href,
      type: document._metadata.type
    };
    deferred.resolve(blobData);
  });
  return deferred.promise;
}

function storeEntity(blobData) {
  const urlEntity = {
    PartitionKey: { '_': `url:${blobData.type}` },
    RowKey: { '_': encodeURIComponent(blobData.url) },
    blobName: { '_': blobData.name }
  };
  const urnEntity = {
    PartitionKey: { '_': `urn:${blobData.type}` },
    RowKey: { '_': encodeURIComponent(blobData.urn) },
    blobName: { '_': blobData.name }
  };
  const insertEntity = Q.nbind(tableService.insertEntity, tableService);
  return Q.allSettled([
    insertEntity(jobConfig.azureStorage.name, urlEntity),
    insertEntity(jobConfig.azureStorage.name, urnEntity)
  ]).then(([urlResult, urnResult]) => {
    updateStats(urlResult, stats.url);
    updateStats(urnResult, stats.urn);
    return Q();
  });
}

function updateStats(result, statistics) {
  if (result.state === 'fulfilled') {
    statistics.added++;
  }
  if (result.state === 'rejected') {
    statistics.skipped++;
    if (result.reason.code === 'EntityAlreadyExists') {
      statistics.skippedReason.alreadyExists++;
    } else {
      statistics.skippedReason.errors++;
      console.error(result.reason);
    }
  }
}

function startNext() {
  if (continuationToken) {
    return start();
  }
  return completeRun();
}

function completeRun() {
  console.log(stats);
  console.timeEnd('Populate table mapping');
  return Q();
}
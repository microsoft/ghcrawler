// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/* This script is a point-in-time script that migrates traffic data from Azure blob storage produced by
   the deprecated Traffic API job to stores used by ghcrawler.

  Optional argument: continuation token value.
  Example: node jobs/oneTimeMigrateTrafficData.js '{"nextMarker":"VALUE","targetLocation":0}'
*/

const AzureStorage = require('azure-storage');
const config = require('painless-config');
const CrawlerFactory = require('../lib/crawlerFactory');
const Q = require('q');
const qlimit = require('qlimit');

const jobConfig = {
  azureStorage: {
    account: config.get('CRAWLER_DELTA_STORAGE_ACCOUNT'),
    key: config.get('CRAWLER_DELTA_STORAGE_KEY'),
    name: 'githubtraffic'
  }
};

const stats = {
  batches: 0,
  total: 0
};

const blobsLimit = qlimit(1);
const storageLimit = qlimit(10);
const retryOperations = new AzureStorage.ExponentialRetryPolicyFilter();
const blobService = AzureStorage.createBlobService(jobConfig.azureStorage.account, jobConfig.azureStorage.key).withFilter(retryOperations);
let continuationToken = process.argv[2] ? JSON.parse(process.argv[2]) : null;
let store = null;

console.time('Traffic data migration');

createStore();
start();

function start() {
  return Q().then(retrieveBlobNames)
    .then(processBatch)
    .catch(error => console.error(error))
    .then(startNext);
}

function createStore() {
  const storageOptions = {
    ttl: 3 * 1000,
    provider: config.get('CRAWLER_STORE_PROVIDER') || 'azure',
    delta: {
      provider: config.get('CRAWLER_DELTA_PROVIDER')
    }
  }
  store = CrawlerFactory.createStore(storageOptions);
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
  return Q.all(blobNames.map(blobsLimit(blobName => {
    stats.total++;
    return getBlobData(blobName)
      .then(splitEntities)
      .then(transformEntities)
      .then(storeDocuments);
  }))).then(() => {
    console.timeEnd(`Processed batch ${stats.batches}`);
    return Q();
  });
}

function getBlobData(blobName) {
  const deferred = Q.defer();
  blobService.getBlobToText(jobConfig.azureStorage.name, blobName, (error, text) => {
    if (error) {
      return Q.reject(error);
    }
    deferred.resolve({ blobName, text });
  });
  return deferred.promise;
}


function splitEntities({ blobName, text }) {
  console.log('Blob size:', text.length);
  const entities = text.split('\n').filter(line => line);
  return Q({ blobName, entities });
}

function transformEntities({ blobName, entities }) {
  console.log(`${blobName}: ${entities.length} docs`);
  const documents = [];
  const date = blobName.replace(/^\D+/g, '').replace('.json', '');
  for (let entity of entities) {
    const doc = JSON.parse(entity);
    if (!doc.category || !doc.repoId || !doc.date) { // Protection against bad data
      console.log(`Skipping ${entity} in ${blobName} due to missing data.`);
      continue;
    }
    let outputDocument = doc.data;
    if (['paths', 'referrers'].includes(doc.category)) {
      outputDocument = {
        elements: doc.data
      };
    }
    outputDocument.id = date;
    outputDocument._metadata = {
      type: doc.category,
      url: `https://api.github.com/repos/${doc.repo + getTrafficUrlPart(doc.category)}`,
      fetchedAt: doc.date,
      links: {
        self: {
          href: `urn:repo:${doc.repoId}:${doc.category}:${date}`,
          type: 'resource'
        },
        repo: {
          href: `urn:repo:${doc.repoId}`,
          type: 'resource'
        }
      },
      version: 13,
      processedAt: doc.date
    };
    documents.push(outputDocument);
  }
  return Q(documents);
}

function getTrafficUrlPart(type) {
  switch (type) {
    case 'clones':
      return '/traffic/clones';
    case 'paths':
      return '/traffic/popular/paths';
    case 'referrers':
      return '/traffic/popular/referrers';
    case 'views':
      return '/traffic/views';
  }
  throw new Error('type not found');
}

function storeDocuments(documents) {
  return Q.all(documents.map(storageLimit(document => {
    return store.upsert(document);
  })));
}

function startNext() {
  if (continuationToken) {
    return start();
  }
  return completeRun();
}

function completeRun() {
  console.log(stats);
  console.timeEnd('Traffic data migration');
  return Q();
}
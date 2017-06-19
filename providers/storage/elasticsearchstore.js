// Copyright (c) 2017, The Linux Foundation. All rights reserved.
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. 

var elasticsearch = require('elasticsearch');
const promiseRetry = require('promise-retry');

const indexname = 'github';


class ElasticsearchStore {

  constructor(url, options) {
    this.url = url;
    this.options = options;
  }

  connect() {
    return promiseRetry((retry, number) => {
      var value = new elasticsearch.Client({host:this.url});
      this.client = value;
      return this.client.ping({requestTimeout: 30000}).catch(retry);
    });
  }

  upsert(document) {
    const selfHref = document._metadata.links.self.href;
    const type = document._metadata.type;
    this.options.logger.info("Upsert document in Elastic Search");
    return this.client.index({
      index: indexname,
      id: document._metadata.url,
      type: type,
      body: document
    }).then((err, resp, respcode) => {
      return resp;
    });
  }

  get(type, url) {
    this.options.logger.info("Get document from Elastic Search");
    return this.client.get({
      index: indexname,
      type: type,
      id: url
    }).then((err, resp, respcode) => {
      if (resp) {
        return resp._source;
      }
      return null;
    });
  }

  etag(type, url) {
    return this.client.get({
      index: indexname,
      type: type,
      id: url,
      ignore: [404]
    }).then((err, resp) => {
      if (resp) {
        return resp._metadata.etag;
      }
      return null;
    });
  }

  list(type) {
    return this.client.search({'index':indexname, 'type':type}).then((error, response) => {
      return result;
    });
  }

  delete(type, urn) {
    return this.client.delete({'index':indexname, '_metadata.links.self.href':urn}).then((error, response)=> {
      return response;
    });
  }

  count(type) {
    return this.client.count({'index': indexname});
  }

  close() {
    this.client.close();
  }

}

module.exports = ElasticsearchStore;

const moment = require('moment');
class FileProcessor {
  constructor(store, files) {
    this.store = store;
    this.filesCollection = files;
    this.version = 0;
  }

  process(request) {
    const handler = this.getHandler(request);
    const result = handler.call(this, request);
    if (result) {
      result._metadata.version = this.version;
      result._metadata.processedAt = moment.utc().toISOString();
    }
    return result;
  }

  getHandler(request, type = request.type) {
    return (this[type]);
  }

  commit(request) {
    const document = request.document;
    request.addSelfLink('sha');
    this._addRoot(request, 'repo', 'repo', document.url.replace(/\/commits\/.*/, ''), `${request.context.qualifier}`);
    if (document.files) {
      const processableFiles = document.files.filter(file => { return this.filesCollection.indexOf(file.filename) > -1; })
      processableFiles.forEach(file => {
       request.queueChild('file', file.contents_url, document._metadata.links.self.href);
      });
    }
    return document;
  }

  files(request) {
    const document = request.document;
    this.filesCollection.forEach(file => {
      request.queueChild('file', `${request.url}/contents/${file}`, `urn:repo:${document.id}`);
    });
    return null;
  }

  file(request) {
    request.document.id = `${request.document.name}_${moment.utc(request.document._metadata.fetchedAt).format('YYYY_MM_DD')}`;
    request.addSelfLink();
    request.linkResource('repo', request.context.qualifier);
    return request.document;
  }

  _addRoot(request, name, type, url = null, urn = null) {
    const element = request.document[name];
    // If there is no element then we must have both the url and urn as otherwise we don't know how to compute them
    if (!element && !(urn && url)) {
      return;
    }

    urn = urn || `urn:${type}:${element.id}`;
    url = url || element.url;
    request.linkResource(name, urn);
    request.queueRoot(type, url);
  }
}

module.exports = FileProcessor;
const moment = require('moment');
class FileProcessor {
  constructor(store, files) {
    this.store = store;
    this.filesCollection = files;
    this.version = 0;
  }

  process(request) {
    const handler = this._getHandler(request);
    const result = handler.call(this, request);
    if (result) {
      result._metadata.version = this.version;
      result._metadata.processedAt = moment.utc().toISOString();
    }
    return result;
  }

  commit(request) {
    const document = request.document;
    request.addSelfLink('sha');
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
    request.document.id = request.document.name;
    //this sucks, refactor later but the parent for all files queued needs to be commit sha
    if (request.context.qualifier.indexOf('commit') === -1) {
      request.context.qualifier = `${request.context.qualifier}:commit:${request.document.sha}`;
    }
    request.addSelfLink();
    request.linkSiblings(request.context.qualifier + ':file');
    return request.document;
  }

  canHandle(request) {
    return !!this._getHandler(request) && request.policy.shouldProcess(request, this.version);
  }

  _getHandler(request, type = request.type) {
    return (this[type]);
  }
}

module.exports = FileProcessor;
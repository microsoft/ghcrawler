const moment = require('moment');
class FileProcessor {
  constructor(store, files) {
    this.store = store;
    this.filesCollection = files;
    this.version = 0;
  }

  process(request) {
    const handler = this.getHandler(request);

    if (!request.policy.shouldProcess(request, this.version)) {
      request.markSkip('Excluded', `Traversal policy excluded this resource`);
      return request.document;
    }

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
}

module.exports = FileProcessor;
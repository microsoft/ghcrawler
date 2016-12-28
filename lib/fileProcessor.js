const moment = require('moment');
class FileProcessor {
  constructor(store) {
    this.store = store;
    this.version = 0;
  }

  process(request) {
    const handler = this._getHandler(request);
    if (!handler) {
      request.markSkip('Skip', `No handler found for request type: ${request.type}`);
      return request.document;
    }

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

  _getHandler(request, type = request.type) {
    return (this[type]);
  }

  file(request) {
    console.log('Processed file request - YAY!');
    return request.document;
  }
}
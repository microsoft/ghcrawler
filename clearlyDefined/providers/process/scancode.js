// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const tmp = require('tmp');

class ScanCodeProcessor {
  getHandler(request, type = request.type) {
    return type === 'scancode' ? this._scan.bind(this) : null;
  }

  _scan(request) {
    request.addRootSelfLink(this._getConfigurationId());
    const dir = this._createTempLocation(request);

    console.log(`Running ScanCode on ${request.document.location} with output going to ${dir.name}`);
    request.document.output = dir.name;
    return new Promise((resolve, reject) => {
      // TODO really run the scan here
      require('fs').appendFile(request.document.output, 'this is some scancode output', error => {
        error ? reject(error) : resolve(request);
      });
    });
  }

  _createTempLocation(request) {
    const result = tmp.fileSync({ unsafeCleanup: true, prefix: 'cd-' });
    request.trackCleanup(result.removeCallback);
    return result;
  }

  _getConfigurationId() {
    return `scancode--${this._getVersion()}`;
  }

  _getVersion() {
    // TODO get the real version of the configured tool
    return '1';
  }
}

module.exports = () => new ScanCodeProcessor();
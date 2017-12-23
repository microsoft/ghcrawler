// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const URL = require('url');
const Git = require('nodegit');
const tmp = require('tmp');

const providerMap = {
  github: "https://github.com"
}

class GitHubCloner {

  constructor() {
  }


  getHandler(request, type = request.type) {
    return this._fetch.bind(this);
  }

  async _fetch(request) {
    const segments = URL.parse(request.url).path.split('/');
    const name = `${segments[3]}/${segments[4]}`;
    const revision = segments.length === 5 ? null : segments[5];

    const url = this._buildUrl(name);
    const options = this._buildOptions(revision);
    const dir = this._createTempLocation(request);

    const repo = await Git.Clone(url, dir.name, options)

    request.contentOrigin = 'origin';
    request.document = this._createCloneRecordDocument(request, dir, repo);
    request.trackCleanup(dir.removeCallback);
    return request;
  }

  _buildOptions(version) {
    return { version };
  }

  _buildUrl(name) {
    return `https://github.com/${name}.git`
  }

  _createTempLocation(request) {
    return tmp.dirSync({ unsafeCleanup: true, prefix: 'cd-' });
  }

  _createCloneRecordDocument(request, dir, repo) {
    const url = URL.parse(request.url);
    const id = url.path.slice(1).replace('/', ':');
    return {
      id,
      location: dir.name,
      repo
    }
  }
}

module.exports = options => new GitHubCloner(options);
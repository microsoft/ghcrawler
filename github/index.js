// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

module.exports = {
  fetch: {
    github: require('./providers/fetch/githubFetcher')
  },
  process: {
    github: require('./providers/process/githubProcessor')
  }
}

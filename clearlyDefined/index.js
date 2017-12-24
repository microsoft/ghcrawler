// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

module.exports = {
  fetch: {
    git: require('./providers/fetch/gitCloner')
  },
  process: {
    scancode: require('./providers/process/scancode')
  },
  store: {
    clearlyDefined: require('./providers/store/clearlyDefined')
  }
}

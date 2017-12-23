// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

module.exports = {
  fetcher: {
    github: require('./providers/fetcher/github')
  },
  processor: {
    scancode: require('./providers/processor/scancode')
  },
  store: {
    clearlyDefined: require('./providers/store/clearlyDefined')
  }
}

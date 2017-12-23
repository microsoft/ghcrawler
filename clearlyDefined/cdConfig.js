// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const config = require('painless-config');

const clearlyDefined = {
  url: config.get('CRAWLER_STORE_URL'),
  token: config.get('CRAWLER_STORE_TOKEN')
};

module.exports =
  {
    crawler: {
      count: 1
    },
    fetch: {
      github: {}
    },
    process: {
      scancode: {}
    },
    store: {
      provider: 'clearlyDefined',
      clearlyDefined
    },
    deadletter: {
      provider: 'clearlyDefined',
      clearlyDefined
    },
    queue: {
      provider: 'memory',
      memory: {
        weights: { immediate: 3, soon: 2, normal: 3, later: 2 }
      }
    }
  };


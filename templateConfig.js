// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

module.exports =
  {
    crawler: {
      count: 1
    },
    fetch: {
    },
    process: {
    },
    store: {
      provider: 'memory'
    },
    deadletter: {
      provider: 'memory'
    },
    lock: {
      provider: 'memory'
    },
    queue: {
      provider: 'memory',
      memory: {
        weights: { immediate: 3, soon: 2, normal: 3, later: 2 }
      }
    },
    redis: {
      provider: 'redis',
      redis: {
        url: config.get('CRAWLER_REDIS_URL'),
        port: config.get('CRAWLER_REDIS_PORT'),
        key: config.get('CRAWLER_REDIS_ACCESS_KEY'),
        tls: config.get('CRAWLER_REDIS_TLS') === 'true'
      },
      memory: {}
    }
  };


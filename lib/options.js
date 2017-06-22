const config = require('painless-config');
const fs = require('fs');

function loadLines(path) {
  if (!path || !fs.existsSync(path)) {
    return [];
  }
  let result = fs.readFileSync(path, 'utf8');
  result = result.split(/\s/);
  return result.filter(line => { return line; }).map(line => { return line.toLowerCase(); });
}

function loadOrgs() {
  let orgList = config.get('CRAWLER_ORGS');
  if (orgList) {
    orgList = orgList.split(';').map(entry => entry.toLowerCase().trim());
  } else {
    orgList = loadLines(config.get('CRAWLER_ORGS_FILE'));
  }
  return orgList;
}

function getDefaultOptions() {
  return {
    crawler: {
      name: config.get('CRAWLER_NAME') || 'crawler',
      count: 0,
      pollingDelay: 5000,
      processingTtl: 60 * 1000,
      promiseTrace: false,
      requeueDelay: 5000,
      orgList: loadOrgs(),
      deadletterPolicy: 'always' // Another option: excludeNotFound
    },
    fetcher: {
      tokenLowerBound: 50,
      metricsStore: 'redis',
      callCapStore: 'memory',
      callCapWindow: 1,       // seconds
      callCapLimit: 30,       // calls
      computeLimitStore: 'memory',
      computeWindow: 15,      // seconds
      computeLimit: 15000,    // milliseconds
      baselineFrequency: 60,  // seconds
      deferDelay: 500
    },
    queuing: {
      provider: config.get('CRAWLER_QUEUE_PROVIDER') || 'amqp10',
      queueName: config.get('CRAWLER_QUEUE_PREFIX') || 'crawler',
      credit: 100,
      weights: { events: 10, immediate: 3, soon: 2, normal: 3, later: 2 },
      messageSize: 240,
      parallelPush: 10,
      pushRateLimit: 200,
      metricsStore: 'redis',
      events: {
        provider: config.get('CRAWLER_EVENT_PROVIDER') || 'webhook',
        topic: config.get('CRAWLER_EVENT_TOPIC_NAME') || 'crawler',
        queueName: config.get('CRAWLER_EVENT_QUEUE_NAME') || 'crawler'
      },
      attenuation: {
        ttl: 3000
      },
      tracker: {
        // driftFactor: 0.01,
        // retryCount: 3,
        // retryDelay: 200,
        // locking: true,
        // lockTtl: 1000,
        ttl: 60 * 60 * 1000
      }
    },
    storage: {
      ttl: 3 * 1000,
      provider: config.get('CRAWLER_STORE_PROVIDER') || 'azure',
      delta: {
        provider: config.get('CRAWLER_DELTA_PROVIDER')
      }
    },
    locker: {
      provider: 'redis',
      retryCount: 3,
      retryDelay: 200
    }
  };
}

exports.getDefaultOptions = getDefaultOptions;

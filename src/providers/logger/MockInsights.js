// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const appInsights = require("applicationinsights");

class MockInsights {
  constructor(client = null) {
    this.client = client;
  }

  static setup(key = null, echo = false) {
    // exit if we we are already setup
    if (appInsights.client instanceof MockInsights) {
      return;
    }
    if (!key || key === 'mock') {
      appInsights.client = new MockInsights();
    } else {
      appInsights
        .setup(key)
        .setAutoCollectPerformance(false)
        .setAutoCollectDependencies(false)
        .start();
      if (echo) {
        appInsights.client = new MockInsights(appInsights.client);
      }
    }
  }

  trackEvent(name, properties, measurements) {
    console.log(`Event: ${name}, properties: ${JSON.stringify(properties)}`);
    if (this.client) {
      this.client.trackEvent(name, properties, measurements);
    }
  }

  trackException(error, properties) {
    console.log('trackException:');
    console.dir(error);
    properties = properties || {};
    if (error && error._type) {
      properties.type = error._type;
      properties.url = error._url;
      properties.cid = error._cid;
    }
    if (this.client) {
      this.client.trackException(error, properties);
    }
  }

  trackMetric(name, value, count, min, max, stdDev) {
    console.log(`Metric: ${name} = ${value}`);
    if (this.client) {
      this.client.trackMetric(name, value, count, min, max, stdDev);
    }
  }

  trackRequest(request, response, properties) {
    console.log('Request: ');
    if (this.client) {
      this.client.trackRequest(request, response, properties);
    }
  }

  trackTrace(message, severityLevel = 1, properties = null) {
    // const severities = ['Verbose', 'Info', 'Warning', 'Error', 'Critical'];
    const severities = ['V', 'I', 'W', 'E', 'C'];
    const hasProperties = properties && Object.keys(properties).length > 0;
    const propertyString = hasProperties ? `${JSON.stringify(properties)}` : '';
    console.log(`[${severities[severityLevel]}] ${message}${propertyString}`);
    if (this.client) {
      this.client.trackTrace(message, severityLevel, properties);
    }
  }

  trackDependency(name, commandName, elapsedTimeMs, success, dependencyTypeName, properties, dependencyKind, async, dependencySource) {
    console.log(`Dependency: ${name}`);
    if (this.client) {
      this.client.trackDependency(name, commandName, elapsedTimeMs, success, dependencyTypeName, properties, dependencyKind, async, dependencySource);
    }
  }
}
module.exports = MockInsights;
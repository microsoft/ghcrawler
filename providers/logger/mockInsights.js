// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const appInsights = require('applicationinsights');

class MockInsights {
  constructor(client = null) {
    this.client = client;
  }

  static setup(key = null, echo = false) {
    // exit if we we are already setup
    if (appInsights.defaultClient instanceof MockInsights) {
      return;
    }
    if (!key || key === 'mock') {
      appInsights.defaultClient = new MockInsights();
    } else {
      appInsights
        .setup(key)
        .setAutoCollectPerformance(false)
        .setAutoCollectDependencies(false)
        .start();
      if (echo) {
        appInsights.defaultClient = new MockInsights(appInsights.defaultClient);
      }
    }
  }

  trackEvent(eventTelemetry) {
    console.log(`Event: ${eventTelemetry.name}, properties: ${JSON.stringify(eventTelemetry.properties)}`);
    if (this.client) {
      this.client.trackEvent(eventTelemetry);
    }
  }

  trackException(exceptionTelemetry) {
    console.log('trackException:');
    console.dir(exceptionTelemetry.exception);
    exceptionTelemetry.properties = exceptionTelemetry.properties || {};
    if (exceptionTelemetry.exception && exceptionTelemetry.exception._type) {
      exceptionTelemetry.properties.type = error._type;
      exceptionTelemetry.properties.url = error._url;
      exceptionTelemetry.properties.cid = error._cid;
    }
    if (this.client) {
      this.client.trackException(exceptionTelemetry);
    }
  }

  trackMetric(metricTelemetry) {
    console.log(`Metric: ${metricTelemetry.name} = ${metricTelemetry.value}`);
    if (this.client) {
      this.client.trackMetric(metricTelemetry);
    }
  }

  trackRequest(requestTelemetry) {
    console.log('Request: ');
    if (this.client) {
      this.client.trackRequest(requestTelemetry)
    }
  }

  trackTrace(traceTelemetry) {
    // const severities = ['Verbose', 'Info', 'Warning', 'Error', 'Critical'];
    const severities = ['V', 'I', 'W', 'E', 'C'];
    const hasProperties = traceTelemetry.properties && Object.keys(traceTelemetry.properties).length > 0;
    const propertyString = hasProperties ? `${JSON.stringify(traceTelemetry.properties)}` : '';
    console.log(`[${severities[traceTelemetry.severity]}] ${traceTelemetry.message}${propertyString}`);
    if (this.client) {
      this.client.trackTrace(traceTelemetry);
    }
  }

  trackDependency(dependencyTelemetry) {
    console.log(`Dependency: ${dependencyTelemetry.name}`);
    if (this.client) {
      this.client.trackDependency(dependencyTelemetry)
    }
  }
}
module.exports = MockInsights;

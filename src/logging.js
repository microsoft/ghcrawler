const config = require('painless-config');
const winston = require('winston');
const MockInsights = require('./providers/logger/MockInsights');
const appInsights = require('applicationinsights');
const aiLogger = require('winston-azure-application-insights').AzureApplicationInsightsLogger;

function createLogger(echo = false, level = 'info') {
  MockInsights.setup(config.get('CRAWLER_INSIGHTS_KEY') || 'mock', echo);
  const result = new winston.Logger();
  result.add(aiLogger, {
    insights: appInsights,
    treatErrorsAsExceptions: true,
    exitOnError: false,
    level: level
  });
  return result;
}

exports.createLogger = createLogger;
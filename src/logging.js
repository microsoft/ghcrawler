const config = require('painless-config');
const winston = require('winston');
const mockInsights = require('./providers/logger/mockInsights');
const appInsights = require('applicationinsights');
const aiLogger = require('winston-azure-application-insights').AzureApplicationInsightsLogger;

function createLogger(echo = false, level = 'info') {
  mockInsights.setup(config.get('CRAWLER_INSIGHTS_KEY') || 'mock', echo);
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
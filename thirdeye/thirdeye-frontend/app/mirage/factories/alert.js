import { Factory } from 'ember-cli-mirage';

export default Factory.extend({
  version: 1,
  createdBy: null,
  updatedBy: null,
  collection(i) { return `test_collection_${i+1}`; },
  functionName(i) { return `test_function_${i+1}`; },
  metric(i) { return `test_metric_${i+1}`; },
  metrics(i) { return [`test_metric_${i+1}`]; },
  properties(i) { return `test_properties_${i+1}`; },
  cron(i) { return `test_cron_${i+1}`; },
  metricFunction: "SUM",
  type: "WEEK_OVER_WEEK_RULE",
  isActive: true,
  globalMetric: null,
  globalMetricFilters: null,
  frequency: {
    size: 1,
    unit: "DAYS"
  },
  bucketSize: 1,
  bucketUnit: "DAYS",
  windowSize: 7,
  windowUnit: "DAYS",
  metricId: 0,
  dataFilter: null,
  alertFilter: null,
  anomalyMergeConfig: null,
  requiresCompletenessCheck: true,
  topicMetric: "test_metric",
  toCalculateGlobalMetric: false
});

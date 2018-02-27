import { Factory } from 'ember-cli-mirage';

export default Factory.extend({
  version: 1,
  createdBy: null,
  updatedBy: null,
  collection: "test_collection_1",
  functionName: "test_function_1",
  metric: "test_metric_1",
  metrics: ["test_metric_1"],
  metricFunction: "SUM",
  type: "WEEK_OVER_WEEK_RULE",
  isActive: true,
  globalMetric: null,
  globalMetricFilters: null,
  properties: "test_properties_1",
  cron: "test_cron_1",
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

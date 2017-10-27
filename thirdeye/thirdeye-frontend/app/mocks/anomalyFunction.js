/**
 * Mocks a list of alerts, displayed in the /manage/alerts page
 */
export default [
  {
    id: 10,
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
  }, {
    id: 20,
    version: 1,
    createdBy: null,
    updatedBy: null,
    collection: "test_collection_2",
    functionName: "test_function2",
    metric: "test_metric_2",
    metrics: ["test_metric_2"],
    metricFunction: "SUM",
    type: "WEEK_OVER_WEEK_RULE",
    isActive: true,
    globalMetric: null,
    globalMetricFilters: null,
    properties: "test_properties_2",
    cron: "test_cron_2",
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
    topicMetric: "test_metric_2",
    toCalculateGlobalMetric: false
  }];

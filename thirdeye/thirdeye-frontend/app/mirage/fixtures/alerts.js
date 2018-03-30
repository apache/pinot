export default [
  {
    id: 1,
    version: 1,
    createdBy: 'someone@linkedin.biz',
    updatedBy: 'test-user',
    collection: 'test_collection_1',
    functionName: 'test_function_1',
    metric: 'test_metric_1',
    metrics: [ 'test_metric_1' ],
    isActive: true,
    frequency: {
      size: 1,
      unit: 'HOURS'
    },
    bucketSize: 1,
    bucketUnit: 'HOURS',
    windowSize: 24,
    windowUnit: 'HOURS',
    windowDelay: 0,
    windowDelayUnit: 'HOURS',
    metricId: 1947794,
    alertFilter: {
      autoTuneType: 'AUTOTUNETYPETEST',
      pattern: 'UP,DOWN'
    },
    toCalculateGlobalMetric: false
  }
];

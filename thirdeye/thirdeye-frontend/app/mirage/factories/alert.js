import { Factory } from 'ember-cli-mirage';

const ALERT_RULES = [
  {
    detection: [
      {
        name: 'detection_rule_2',
        type: 'ABSOLUTE_CHANGE_RULE',
        params: {
          offset: 'wo1w',
          change: 1,
          pattern: null
        }
      },
      {
        name: 'filter_rule_2',
        type: 'ALGORITHM_FILTER',
        params: {
          configuration: {
            bucketPeriod: null
          }
        }
      }
    ]
  }
];

const ALERT_FILTERS = {
  dimensionName1: ['dimensionValue1', 'dimensionValue2'],
  dimensionName2: ['dimensionValue3']
};

const ALERT_DIMENSION_EXPLORATION = {
  dimensions: ['dimensionName'],
  minContribution: 0.05,
  k: 10
};

const ALERT_DATASET_NAMES = ['test_dataset'];

const ALERT_MONITORING_GRANULARITY = ['1_DAY'];

export default Factory.extend({
  metric(i) {
    return `test_metric_${i + 1}`;
  },
  functionName(i) {
    return `test_function_${i + 1}`;
  },
  subscriptionGroup(i) {
    return [`test_sub_${i + 1}`];
  },
  createdBy: null,
  name(i) {
    return `test_function_${i + 1}`;
  },
  rules: ALERT_RULES,
  filters: ALERT_FILTERS,
  pipelineType: 'Composite',
  active: true,
  dataset: null,
  dimensionExploration: ALERT_DIMENSION_EXPLORATION,
  datasetNames: ALERT_DATASET_NAMES,
  monitoringGranularity: ALERT_MONITORING_GRANULARITY
});

import { Factory } from 'ember-cli-mirage';

export default Factory.extend({
  metric(i) { return `test_metric_${i+1}`; },
  functionName(i) { return `test_function_${i+1}`; },
  createdBy : null,
  detectionName(i) { return `test_function_${i+1}`; },
  rules : [ {
    detection : [ {
      name : "detection_rule_2",
      type : "ABSOLUTE_CHANGE_RULE",
      params : {
        offset : "wo1w",
        change : 1,
        pattern : null
      }
    }, {
      name : "filter_rule_2",
      type : "ALGORITHM_FILTER",
      params : {
        configuration : {
          bucketPeriod : null
        }
      }
    } ]
  } ],
  filters : {
    dimensionName1 : [ "dimensionValue1", "dimensionValue2" ],
    dimensionName2 : [ "dimensionValue3" ]
  },
  pipelineType : "Composite",
  active : true,
  dataset : null,
  dimensionExploration : {
    dimensions : [ "dimensionName" ],
    minContribution : 0.05,
    k : 10
  }
});

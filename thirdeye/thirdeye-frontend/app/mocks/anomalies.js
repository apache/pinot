/**
 * Mock anomalies payload in self-serve
 */

export default {
  "anomalyIds" : [ 38456269, 382977991, 38293331 ],
  "searchFilters" : {
    "statusFilterMap" : {
      "Not Resolved" : [ 38456269, 382977991, 38293331 ]
    },
    "functionFilterMap" : {
      "thirdeyeDemo_testMetric1_upDown_hours02" : [ 38456269, 382977991, 38293331 ]
    },
    "datasetFilterMap" : {
      "demoDataset" : [ 38456269, 382977991, 38293331 ]
    },
    "metricFilterMap" : {
      "testMetric1" : [ 38456269, 382977991, 38293331 ]
    },
    "dimensionFilterMap" : { },
    "issueTypeFilterMap" : { }
  },
  "anomalyDetailsList" : [ {
    "anomalyId" : 38293331,
    "metric" : "testMetric1",
    "metricId" : 19467300,
    "dataset" : "demoDataset",
    "timeUnit" : "HOURS",
    "externalUrl" : "{}",
    "dates" : [ "2018-02-28 11:00", "2018-02-28 15:00", "2018-02-28 19:00", "2018-02-28 23:00"],
    "currentEnd" : "Mar 9 2018 12:00",
    "currentStart" : "Feb 28 2018 08:00",
    "baselineValues" : [ "2978.583576642332", "2777.633576642332", "2688.6710766423325", "2775.033576642333" ],
    "currentValues" : [ "2558.25", "2219.5", "2042.75", "2473.25" ],
    "current" : "2364.25",
    "baseline" : "3072.77",
    "anomalyStart" : "2018-03-07 08:00",
    "anomalyEnd" : "2018-03-08 12:00",
    "anomalyRegionStart" : "2018-03-07 07:30",
    "anomalyRegionEnd" : "2018-03-08 11:30",
    "anomalyFunctionId" : 38289963,
    "anomalyFunctionName" : "thirdeyeDemo_testMetric1_upDown_hours02",
    "anomalyFunctionType" : "DETECTION_MODEL_TYPE",
    "anomalyFunctionProps" : "",
    "anomalyFunctionDimension" : "{}"
  }, {
    "anomalyId" : 382977991,
    "metric" : "testMetric1",
    "metricId" : 19467300,
    "dataset" : "demoDataset",
    "timeUnit" : "HOURS",
    "externalUrl" : "{}",
    "dates" : [ "2018-02-28 11:00", "2018-02-28 15:00", "2018-02-28 19:00", "2018-02-28 23:00"],
    "currentEnd" : "Mar 9 2018 12:00",
    "currentStart" : "Feb 28 2018 08:00",
    "baselineValues" : [ "2978.583576642332", "2777.633576642332", "2688.6710766423325", "2775.033576642333" ],
    "currentValues" : [ "2558.25", "2219.5", "2042.75", "2473.25" ],
    "current" : "2364.25",
    "baseline" : "2358.09",
    "anomalyStart" : "2018-03-06 01:00",
    "anomalyEnd" : "2018-03-07 00:00",
    "anomalyRegionStart" : "2018-03-06 00:30",
    "anomalyRegionEnd" : "2018-03-06 23:30",
    "anomalyFunctionId" : 38289963,
    "anomalyFunctionName" : "thirdeyeDemo_testMetric1_upDown_hours02",
    "anomalyFunctionType" : "DETECTION_MODEL_TYPE",
    "anomalyFunctionProps" : "",
    "anomalyFunctionDimension" : "{}"
  }, {
    "anomalyId" : 38456269,
    "metric" : "testMetric1",
    "metricId" : 19467300,
    "dataset" : "demoDataset",
    "timeUnit" : "HOURS",
    "externalUrl" : "{}",
    "dates" : [ "2018-02-28 11:00", "2018-02-28 15:00", "2018-02-28 19:00", "2018-02-28 23:00"],
    "currentEnd" : "Mar 9 2018 12:00",
    "currentStart" : "Feb 28 2018 08:00",
    "baselineValues" : [ "2978.583576642332", "2777.633576642332", "2688.6710766423325", "2775.033576642333" ],
    "currentValues" : [ "2558.25", "2219.5", "2042.75", "2473.25" ],
    "current" : "1906.2",
    "baseline" : "2500.76",
    "anomalyStart" : "2018-03-01 10:00",
    "anomalyEnd" : "2018-03-02 16:00",
    "anomalyRegionStart" : "2018-03-01 09:30",
    "anomalyRegionEnd" : "2018-03-02 15:30",
    "anomalyFunctionId" : 38289963,
    "anomalyFunctionName" : "thirdeyeDemo_testMetric1_upDown_hours02",
    "anomalyFunctionType" : "DETECTION_MODEL_TYPE",
    "anomalyFunctionProps" : "",
    "anomalyFunctionDimension" : "{}"
  } ],
  "totalAnomalies" : 3,
  "numAnomaliesOnPage" : 3
};

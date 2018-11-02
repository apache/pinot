/**
 * Mock anomalies payload in self-serve
 */

function buildAnomalyData(ids) {
  const anomalyList = [];
  ids.forEach((id) => {
    anomalyList.push({
      anomalyId: id,
      metric: 'testMetric1',
      metricId: 19467300,
      dataset: 'demoDataset',
      timeUnit: 'HOURS',
      externalUrl: '{}',
      dates: [ '2018-02-28 11:00', '2018-02-28 15:00', '2018-02-28 19:00', '2018-02-28 23:00'],
      currentEnd: 'Mar 9 2018 12:00',
      currentStart: 'Feb 28 2018 08:00',
      baselineValues: [ '2978.583576642332', '2777.633576642332', '2688.6710766423325', '2775.033576642333' ],
      currentValues: [ '2558.25', '2219.5', '2042.75', '2473.25' ],
      current: '2364.25',
      baseline: '3072.77',
      anomalyStart: '2018-03-07 08:00',
      anomalyEnd: '2018-03-08 12:00',
      anomalyRegionStart: '2018-03-07 07:30',
      anomalyRegionEnd: '2018-03-08 11:30',
      anomalyFunctionId: 38289963,
      anomalyFunctionName: 'thirdeyeDemo_testMetric1_upDown_hours02',
      anomalyFunctionType: 'DETECTION_MODEL_TYPE',
      anomalyFunctionProps: '',
      anomalyFunctionDimension: '{}'
    });
  });
  return anomalyList;
}

export const anomalySet = (ids) => {
  return {
    'anomalyIds': ids,
    'searchFilters': {
      'statusFilterMap': {
        'Not Resolved': ids
      },
      'functionFilterMap': {
        'thirdeyeDemo_testMetric1_upDown_hours02': ids
      },
      'datasetFilterMap': {
        'demoDataset': ids
      },
      'metricFilterMap': {
        'testMetric1': ids
      },
      'dimensionFilterMap': { },
      'issueTypeFilterMap': { }
    },
    'anomalyDetailsList': buildAnomalyData(ids),
    'totalAnomalies': ids.length,
    'numAnomaliesOnPage': ids.length
  };
};

export default {
  anomalySet
};

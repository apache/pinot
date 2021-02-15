import moment from 'moment';
// Mock data that returns an application's anomalies
// TODO: Refactor this into a factory
export default [
  {
    id: 1,
    anomalyId: 33330871,
    start: 1491804013000,
    end: 1491890413000,
    anomalyFeedback: 'True anomaly',
    dimensions: {
      app: 'company',
      country: 'US',
      os: 'Some OS',
      pageName: 'login_page',
      random1: ['partial']
    },
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: 'Yes, but New Trend',
    metricName: 'Metric Name 1',
    metricId: 12345,
    functionName: 'Alert Name 1'
  },
  {
    id: 2,
    anomalyId: 33330876,
    start: 1491804013000,
    end: moment().valueOf(),
    dimensions: {
      country: ['US', 'FR'],
      pageName: ['some_page'],
      random1: ['partial']
    },
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: 'Yes, but New Trend 2',
    metricName: 'Metric Name 1',
    metricId: 12345,
    functionName: 'Alert Name 1'
  },
  {
    id: 3,
    anomalyId: 33330893,
    start: 1491804013000,
    end: moment().valueOf(),
    dimensions: {
      country: ['US', 'FR'],
      pageName: ['some_page'],
      random1: ['partial']
    },
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: 'Yes, but New Trend',
    metricName: 'Metric Name 1',
    metricId: 12345,
    functionName: 'Alert Name 1'
  },
  {
    id: 4,
    anomalyId: 33340923,
    start: 1491804013000,
    end: moment().valueOf(),
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: 'Yes, but New Trend',
    metricName: 'Metric Name 1',
    metricId: 12345,
    functionName: 'Alert Name 1'
  },
  {
    id: 5,
    anomalyId: 33381509,
    start: 1491804013000,
    end: moment().valueOf(),
    dimensions: {
      country: ['US'],
      pageName: ['some_page'],
      random1: ['partial']
    },
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: 'Yes, but New Trend',
    metricName: 'Metric Name 1',
    metricId: 12345,
    functionName: 'Alert Name 1'
  },
  {
    id: 6,
    anomalyId: 33381877,
    start: 1491804013000,
    end: 1491890413000,
    dimensions: {
      country: ['US'],
      pageName: ['some_page'],
      random1: ['partial']
    },
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: 'Yes, but New Trend',
    metricName: 'Metric Name 1',
    metricId: 12345,
    functionName: 'Alert Name 1'
  },
  {
    id: 7,
    anomalyId: 33302982,
    start: 1491804013000,
    end: 1491890413000,
    dimensions: {
      country: ['US', 'FR'],
      pageName: ['some_page'],
      random1: ['partial']
    },
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: 'Yes, but New Trend',
    metricName: 'Metric Name 1',
    metricId: 12345,
    functionName: 'Alert Name 1'
  },
  {
    id: 8,
    anomalyId: 33306049,
    start: 1491804013000,
    end: 1491890413000,
    dimensions: {
      country: 'US',
      pageName: ['some_page'],
      random1: ['partial']
    },
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: 'Yes, but New Trend',
    metricName: 'Metric Name 2',
    metricId: 12345,
    functionName: 'Alert Name 2'
  },
  {
    id: 9,
    anomalyId: 33306058,
    start: 1491804013000,
    end: moment().valueOf(),
    dimensions: {
      country: ['US', 'FR'],
      pageName: ['some_page'],
      random1: ['partial']
    },
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: 'Yes, but New Trend',
    metricName: 'Metric Name 2',
    metricId: 12345,
    functionName: 'Alert Name 2'
  },
  {
    id: 10,
    anomalyId: 33314704,
    start: 1491804013000,
    end: moment().valueOf(),
    dimensions: {
      country: ['US', 'FR'],
      pageName: ['some_page'],
      random1: ['partial']
    },
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: 'Yes, but New Trend',
    metricName: 'Metric Name 2',
    metricId: 12345,
    functionName: 'Alert Name 2'
  },
  {
    id: 11,
    anomalyId: 33314705,
    start: 1491804013000,
    end: moment().valueOf(),
    dimensions: {
      country: ['US', 'FR'],
      pageName: ['some_page'],
      random1: ['partial']
    },
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: 'Yes, but New Trend',
    metricName: 'Metric Name 2',
    metricId: 12345,
    functionName: 'Alert Name 2'
  }
];

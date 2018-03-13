import moment from 'moment';
// Mock data that returns an application's anomalies
// TODO: Refactor this into a factory
export default [
  {
    id: 1,
    start: 1491804013000,
    end: 1491890413000,
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
  }, {
    id: 2,
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
  }, {
    id: 3,
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
  }, {
    id: 4,
    start: 1491804013000,
    end: moment().valueOf(),
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: 'Yes, but New Trend',
    metricName: 'Metric Name 1',
    metricId: 12345,
    functionName: 'Alert Name 1'
  }, {
    id: 5,
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
  }, {
    id: 6,
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
  }, {
    id: 7,
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
  }, {
    id: 8,
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
  }, {
    id: 9,
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
  }, {
    id: 10,
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

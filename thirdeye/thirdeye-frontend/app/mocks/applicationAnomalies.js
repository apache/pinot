// Temporary mock data that returns an application's anomalies until API is ready
export default [
  {
    id: 1,
    start: 12345,
    end: 23456,
    dimensions: {
      country: "US",
      pageName: "d_flagship3_messaging"
    },
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: "Yes, but New Trend",
    metric: "Metric Name",
    functionName: "Alert Name"
  }, {
    id: 2,
    start: 12345,
    end: 23456,
    dimensions: {
      country: "US",
      pageName: "d_flagship3_messaging"
    },
    severity: 0.86,
    current: 1444,
    baseline: 1000,
    feedback: "Yes, but New Trend",
    metric: "Metric Name",
    functionName: "Alert Name"
  }
];

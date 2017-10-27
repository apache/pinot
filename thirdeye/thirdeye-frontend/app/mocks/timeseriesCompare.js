import metric from 'thirdeye-frontend/mocks/metric';
const { name: metricName, id: metricId } = metric[0];
const start = 1506459599999;
const end = 1509048000000;

export default {
  metricName,
  metricId,
  start: 1506459599999,
  end: 1506459599999,
  inverseMetric: false,
  timeBucketsCurrent: Array(721).fill(start),
  timeBucketsBaseline: Array(721).fill(start),
  subDimensionContributionMap: {
    All: {
      currentValues: Array(721).fill(4000),
      baselineValues: Array(721).fill(3000),
      percentageChange: Array(721).fill(1.01),
      cumulativeCurrentValues: Array(721).fill(4000),
      cumulativeBaselineValues: Array(721).fill(7000),
      cumulativePercentageChange: Array(721).fill(1.01)
    }
  }
};

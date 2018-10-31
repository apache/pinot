function TimeSeriesCompareModel() {
  this.metricId;
  this.metricName;

  this.dimension;
  this.filters;

  this.currentStart;
  this.currentEnd;
  this.baselineStart;
  this.baselineEnd;

  this.granularity;

  this.showDetailsChecked = false;
  this.showCumulativeChecked = false;

  this.subDimensions;
  this.subDimensionsIndex = {};
  this.subDimensionContributionDetails;
  this.timeBucketDiff;
  this.heatMapCurrentStart;
  this.heatMapCurrentEnd;

  this.inverseMetric = false;

  this.showTime = () => {
    return this.granularity !== constants.GRANULARITY_DAY;
  };
}

TimeSeriesCompareModel.prototype = {

  init: function (params) {
    if (params) {
      if (params.metricId) {
        this.metricId = params.metricId;
      }
      if (params.metricName) {
        this.metricName = params.metricName;
      }

      this.currentStart = params.currentStart ? moment(params.currentStart) : this.currentStart;
      this.currentEnd = params.currentEnd ? moment(params.currentEnd) : this.currentEnd;
      this.baselineStart = params.baselineStart ? moment(params.baselineStart) : this.baselineStart;
      this.baselineEnd = params.baselineEnd ? moment(params.baselineEnd) : this.baselineEnd;
      this.heatMapCurrentStart = params.heatMapCurrentStart ? moment(params.heatMapCurrentStart) : null;
      this.heatMapCurrentEnd = params.heatMapCurrentEnd ? moment(params.heatMapCurrentEnd) : null;
      this.filters = Object.assign({}, params.filters);
      this.dimension = params.dimension || constants.DEFAULT_ANALYSIS_DIMENSION;
      this.granularity = params.granularity;
    }
  },

  formatDatesForTimeSeries() {
    const granularity = this.granularity;
    const granularityOffset = {
        'DAYS': (date) => {
          return date.clone().startOf('day');
        },
        'HOURS': (date) => {
          return date.clone().startOf('hour');
        },
        '5_MINUTES': (date) => {
          return date.clone().subtract(4, 'minutes').startOf('minute');
        },
      }[granularity];

    return [
      this.currentEnd,
      this.baselineEnd
    ].map(granularityOffset);
  },

  update() {
    if (this.metricId) {
      const [
        currentEnd,
        baselineEnd
      ] = this.formatDatesForTimeSeries();
      // update the timeseries data
      return dataService.fetchTimeseriesCompare(
        this.metricId,
        this.currentStart,
        currentEnd,
        this.baselineStart,
        baselineEnd,
        this.dimension,
        this.filters,
        this.granularity
      ).then((timeSeriesResponse) => {
        if (timeSeriesResponse) {
          var dateColumn = ['date'];
          for (var i in timeSeriesResponse.timeBucketsCurrent) {
            dateColumn.push(timeSeriesResponse.timeBucketsCurrent[i]);
          }
          this.inverseMetric = timeSeriesResponse.inverseMetric;
          if (timeSeriesResponse.subDimensionContributionMap) {
            this.subDimensions = [];
            this.subDimensionContributionDetails = {};
            this.subDimensionContributionDetails.contributionMap = {};
            this.subDimensionContributionDetails.percentageChange = {};
            this.subDimensionContributionDetails.currentValues = {};
            this.subDimensionContributionDetails.baselineValues = {};
            this.subDimensionContributionDetails.cumulativeCurrentValues = {};
            this.subDimensionContributionDetails.cumulativeBaselineValues = {};
            this.subDimensionContributionDetails.cumulativePercentageChange = {};
            this.subDimensionContributionDetails.timeBucketsCurrent = timeSeriesResponse.timeBucketsCurrent;
            this.subDimensionContributionDetails.timeBucketsBaseline = timeSeriesResponse.timeBucketsBaseline;

            var count = 0;
            for (var key in timeSeriesResponse.subDimensionContributionMap) {
              const currentVal = [`${key} current`];
              const baselineVal = [`${key} baseline`];
              // var percentageChange = [];
              if (timeSeriesResponse.subDimensionContributionMap[key]) {
                for (var i in timeSeriesResponse.subDimensionContributionMap[key].currentValues) {
                  currentVal.push(timeSeriesResponse.subDimensionContributionMap[key].currentValues[i]);
                  baselineVal.push(timeSeriesResponse.subDimensionContributionMap[key].baselineValues[i]);
                }
              }
              this.subDimensions.push(key);
              this.subDimensionsIndex[key] = count++;
              this.subDimensionContributionDetails.contributionMap[key] = {
                start: moment(timeSeriesResponse.start).format('YYYY-M-D'),
                end: moment(timeSeriesResponse.end).format('YYYY-M-D'),
                columns: [dateColumn, currentVal, baselineVal],
              };
              this.subDimensionContributionDetails.currentValues[key] = timeSeriesResponse.subDimensionContributionMap[key].currentValues;
              this.subDimensionContributionDetails.baselineValues[key] = timeSeriesResponse.subDimensionContributionMap[key].baselineValues;
              this.subDimensionContributionDetails.cumulativeCurrentValues[key] = timeSeriesResponse.subDimensionContributionMap[key].cumulativeCurrentValues;
              this.subDimensionContributionDetails.cumulativeBaselineValues[key] = timeSeriesResponse.subDimensionContributionMap[key].cumulativeBaselineValues;
              this.subDimensionContributionDetails.percentageChange[key] = timeSeriesResponse.subDimensionContributionMap[key].percentageChange;
              this.subDimensionContributionDetails.cumulativePercentageChange[key] = timeSeriesResponse.subDimensionContributionMap[key].cumulativePercentageChange;
            }
          }
        }

        return timeSeriesResponse;
      });
      // TODO: use time formatter according to granularity selected, currently only DAYS supported
    }
  },
};

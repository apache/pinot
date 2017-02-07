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
}

TimeSeriesCompareModel.prototype = {

  init: function (params) {
    if (params) {
      if (params.metricId) {
        // metric is collection of id / name / alias
        this.metricId = params.metricId;
        // this.metricId = params.metric.id;
        // this.metricName = params.metric.name;
      }
      if (params.currentStart) {
        this.currentStart = params.currentStart;
      }
      if (params.currentEnd) {
        this.currentEnd = params.currentEnd;
      }
      if (params.baselineStart) {
        this.baselineStart = params.baselineStart;
      }
      if (params.baselineEnd) {
        this.baselineEnd = params.baselineEnd;
      }
      if (params.granularity) {
        this.granularity = params.granularity;
      }
      if (params.dimension) {
        this.dimension = params.dimension;
      }
      if (params.filters) {
        this.filters = params.filters;
      }
    }
  },

  update: function () {
    if (this.metricId) {
      // update the timeseries data
      var timeSeriesResponse = dataService.fetchTimeseriesCompare(this.metricId, this.currentStart, this.currentEnd, this.baselineStart, this.baselineEnd, this.dimension, this.filters, this.granularity);

      // TODO: use time formatter according to granularity selected, currently only DAYS supported

      if (timeSeriesResponse) {
        var dateColumn = ['date'];
        for (var i in timeSeriesResponse.timeBucketsCurrent) {
          dateColumn.push(timeSeriesResponse.timeBucketsCurrent[i]);
        }
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
            var currentVal = ['current'];
            var baselineVal = ['baseline'];
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
              columns: [dateColumn, currentVal, baselineVal]
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
    }
  }
};

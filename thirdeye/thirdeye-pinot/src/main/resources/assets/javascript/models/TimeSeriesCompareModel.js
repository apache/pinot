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

  // this.showDetailsChecked = false;
  // this.showCumulativeChecked = false;

  this.subDimensions;
  this.subDimensionContributionDetails;
}

TimeSeriesCompareModel.prototype = {

  init: function (params) {
    if (params) {
      if (params.metric) {
        // metric is collection of id / name / alias
        this.metricId = params.metric.id;
        this.metricName = params.metric.name;
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
    // update the timeseries data
    console.log("timeseries model ---> ");
    console.log(this);
    if (this.metricId) {
      var timeSeriesResponse = dataService.fetchTimeseriesCompare(this.metricId, this.currentStart,
          this.currentEnd, this.baselineStart, this.baselineEnd, this.dimension, this.filters,
          this.granularity);

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
          this.subDimensionContributionDetails.timeBucketsCurrent = timeSeriesResponse.timeBucketsCurrent;

          for (var key in timeSeriesResponse.subDimensionContributionMap) {
            var currentVal = ['current'];
            var baselineVal = ['baseline'];
            var percentageChange = [];
            if (timeSeriesResponse.subDimensionContributionMap[key]) {
              for (var i in timeSeriesResponse.subDimensionContributionMap[key].currentValues) {
                currentVal.push(timeSeriesResponse.subDimensionContributionMap[key].currentValues[i]);
                baselineVal.push(timeSeriesResponse.subDimensionContributionMap[key].baselineValues[i]);
                percentageChange.push(timeSeriesResponse.subDimensionContributionMap[key].percentageChange[i]);
              }
            }
            this.subDimensions.push(key);
            this.subDimensionContributionDetails.contributionMap[key] = {
              start: timeSeriesResponse.start,
              end: timeSeriesResponse.end,
              columns: [dateColumn, currentVal, baselineVal]
            };
            this.subDimensionContributionDetails.percentageChange[key] = percentageChange;
          }
        }
      }
    }
  }
};

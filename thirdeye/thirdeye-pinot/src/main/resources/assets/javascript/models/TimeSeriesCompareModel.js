function TimeSeriesCompareModel() {
  this.metricId;

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
  this.subDimensionContributionMap;
}

TimeSeriesCompareModel.prototype = {

  init: function (params) {
    if (params) {
      if (params.metric) {
        this.metricId = params.metric.id;
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

      var ALL = 'All';

      if (timeSeriesResponse) {
        var dateColumn = ['date'];
        for (var i in timeSeriesResponse.timeBucketsCurrent) {
          // TODO: if granularity is hours or minutes then format accordingly
          dateColumn.push(moment(timeSeriesResponse.timeBucketsCurrent[i]).format('YYYY-M-D'));
        }

        if (timeSeriesResponse.subDimensionContributionMap) {
          this.subDimensions = [];
          this.subDimensionContributionMap = {};
          for (var key in timeSeriesResponse.subDimensionContributionMap) {
            var currentVal = ['current'];
            var baselineVal = ['baseline'];
            if (timeSeriesResponse.subDimensionContributionMap[key]) {
              for (var i in timeSeriesResponse.subDimensionContributionMap[key].currentValues) {
                currentVal.push(timeSeriesResponse.subDimensionContributionMap[key].currentValues[i]);
              }
            }
            if (timeSeriesResponse.subDimensionContributionMap[key]) {
              for (var i in timeSeriesResponse.subDimensionContributionMap[key].baselineValues) {
                baselineVal.push(timeSeriesResponse.subDimensionContributionMap[key].baselineValues[i]);
              }
            }
            this.subDimensions.push(key);
            this.subDimensionContributionMap[key] = {
              start: moment(timeSeriesResponse.start).format('YYYY-M-D'),
              end: moment(timeSeriesResponse.end).format('YYYY-M-D'),
              columns: [dateColumn, currentVal, baselineVal]
            };
          }
        }
      }
    }
  }
};

function TimeSeriesCompareModel() {
  this.metricId;

  this.dimensions;
  this.filters;

  this.currentStart;
  this.currentEnd;
  this.baselineStart;
  this.baselineEnd;

  this.granularity;

  this.timeSeriesObject = {
    'start': '2016-01-3',
    'end': '2016-01-5',
    'columns': [['date', '2016-01-01', '2016-01-2', '2016-01-3', '2016-01-4', '2016-01-05',
      '2016-01-06', '2016-01-07'], ['current', 30, 200, 100, 400, 150, 250, 60],
      ['baseline', 35, 225, 200, 600, 170, 220, 70]]
  };
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
    }
  },

  update: function () {
    // update the timeseries data
    console.log("timeseries model ---> ");
    console.log(this);
    if (this.metricId) {
      var timeSeriesResponse = dataService.fetchTimeseriesCompare(this.metricId, this.currentStart,
          this.currentEnd, this.baselineStart, this.baselineEnd, this.dimensions, this.filters,
          this.granularity);
      if (timeSeriesResponse) {
        // Transform
        this.timeSeriesObject.start = moment(timeSeriesResponse.start).format('YYYY-M-D');
        this.timeSeriesObject.end = moment(timeSeriesResponse.end).format('YYYY-M-D');
        var dateColumn = ['date'];
        var currentVal = ['current'];
        var baselineVal = ['baseline'];
        for (var i in timeSeriesResponse.timeBucketsCurrent) {
          dateColumn.push(moment(timeSeriesResponse.timeBucketsCurrent[i]).format('YYYY-M-D'));
        }
        for (var i in timeSeriesResponse.currentValues) {
          currentVal.push(timeSeriesResponse.currentValues[i]);
        }
        for (var i in timeSeriesResponse.baselineValues) {
          baselineVal.push(timeSeriesResponse.baselineValues[i]);
        }
        this.timeSeriesObject.columns = [dateColumn, currentVal, baselineVal];
      }
    }
  }
};

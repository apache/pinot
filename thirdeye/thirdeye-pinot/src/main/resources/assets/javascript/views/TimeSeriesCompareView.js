function TimeSeriesCompareView(timeSeriesCompareModel) {
  this.timeSeriesCompareModel = timeSeriesCompareModel;
}

TimeSeriesCompareView.prototype = {
  init: function () {
  },

  render: function () {
    // CHART GENERATION
    var chart = c3.generate({
      bindto : '#analysis-chart',
      data : {
        x : 'date',
        columns : this.timeSeriesCompareModel.timeSeriesObject.columns,
        type : 'spline'
      },
      legend : {
        show : false,
        position : 'top'
      },
      axis : {
        y : {
          show : true
        },
        x : {
          type : 'timeseries',
          show : true
        }
      },
      regions : [ {
        start : this.timeSeriesCompareModel.timeSeriesObject.start,
        end : this.timeSeriesCompareModel.timeSeriesObject.end
      } ]
    });

  }
}


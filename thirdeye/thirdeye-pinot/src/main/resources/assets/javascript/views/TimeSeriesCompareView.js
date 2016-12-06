function TimeSeriesCompareView(timeSeriesCompareModel) {
  var template = $("#").html();
  // this.template_compiled = Handlebars.compile(template);
  // this.placeHolderId = "#";
  this.timeSeriesCompareModel = timeSeriesCompareModel;
}

TimeSeriesCompareView.prototype = {

  init: function () {

  },
  
  render: function () {
    // var result = this.template_compiled(this.timeSeriesCompareModel);
    // $(this.placeHolderId).html(result);

    // CHART GENERATION
    var chart = c3.generate({
      bindto : '#analysis-chart',
      data : {
        x : 'date',
        columns : [ [ 'date', '2016-01-01', '2016-01-2', '2016-01-3', '2016-01-4', '2016-01-05', '2016-01-06', '2016-01-07' ], [ 'current', 30, 200, 100, 400, 150, 250, 60 ],
          [ 'baseline', 35, 225, 200, 600, 170, 220, 70 ] ],
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
        start : '2016-01-3',
        end : '2016-01-5'
      } ]
    });

  }


}


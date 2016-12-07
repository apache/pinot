function TimeSeriesCompareModel(params) {
  this.hashParams = params;
  this.timeSeriesObject = {
    'start': '2016-01-3',
    'end': '2016-01-5',
    'columns': [['date', '2016-01-01', '2016-01-2', '2016-01-3', '2016-01-4', '2016-01-05',
      '2016-01-06', '2016-01-07'], ['current', 30, 200, 100, 400, 150, 250, 60],
      ['baseline', 35, 225, 200, 600, 170, 220, 70]]
  };
}

TimeSeriesCompareModel.prototype = {

  init : function (ctx) {

  },

  update: function () {

  }

};

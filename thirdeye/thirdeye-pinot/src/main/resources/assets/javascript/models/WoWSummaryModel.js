function WoWSummaryModel() {
  this.timeRangeLabels = [ "Most Recent Hour", "Today", "Yesterday", "Last 7 Days" ];
  this.wowSummaryList = [];
}

WoWSummaryModel.prototype = {

  init : function(params) {
    this.buildSampleData();
  },
  rebuild : function() {
    // TODO: fetch relevant data from backend
  },
  buildSampleData : function() {
    this.wowSummaryList = [];
    for (i = 0; i < 3; i++) {
      var row = new WoWSummaryRow();
      row.metricName = "metric" + i;
      row.data = [ {
        baseline : 1000,
        current : 2000,
        percentChange : 50,
        startTime : moment().startOf('hour'),
        endTime : moment().startOf('hour').subtract('1', 'hour')
      }, {
        baseline : 1000,
        current : 2000,
        percentChange : -50,
        startTime : moment().startOf('hour'),
        endTime : moment().startOf('day')
      }, {
        baseline : 1000,
        current : 2000,
        percentChange : -10,
        startTime : moment().startOf('day'),
        endTime : moment().startOf('day').subtract('1', 'day')
      }, {
        baseline : 1000,
        current : 2000,
        percentChange : 10,
        startTime : moment().startOf('day'),
        endTime : moment().startOf('day').subtract('7', 'day')
      } ];
      this.wowSummaryList.push(row);
    }

  }
};

function WoWSummaryRow() {
  this.metricName = "N/A";
  this.data = [];
}

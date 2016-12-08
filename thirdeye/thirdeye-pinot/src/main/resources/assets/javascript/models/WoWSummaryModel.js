function WoWSummaryModel() {
  this.startTime = moment().subtract(7, "days");
  this.endTime = moment();
  this.timestamps = [];
  this.wowSummary = [];
}

WoWSummaryModel.prototype = {

  init : function(params) {
    if (params.startTime) {
      this.startTime = params.startTime;
    }
    if (params.endTime) {
      this.endTime = params.endTime;
    }
    this.buildSampleData();
  },
  rebuild : function() {
    // TODO: fetch relevant data from backend
  },
  buildSampleData: function() {
    var row1 = new WoWSummaryRow();
    row1.metricName = "metricD";
    row1.data = [0,0.1,0.2,0,0,0,0,0.1,0.2,0,0,0,0,0.1,0.2,0,0,0,0,0.1,0.2,0,0,0];

    var row2 = new WoWSummaryRow();
    row2.metricName = "metricE";
    row2.data = [0,0.1,0.2,0,0,0,0,0.1,0.2,0,0,0,0,0.1,0.2,0,0,0,0,0.1,0.2,0,0,0];

    var row3 = new WoWSummaryRow();
    row3.metricName = "metricF";
    row3.data = [0,0.1,0.2,0,0,0,0,0.1,0.2,0,0,0,0,0.1,0.2,0,0,0,0,0.1,0.2,0,0,0];

    this.wowSummary = [];
    this.wowSummary.push(row1);
    this.wowSummary.push(row2);
    this.wowSummary.push(row3);

    this.timestamps = [];
    var time = moment(this.endTime).subtract(24, 'hours');
    for (var i = 0; i < 24; ++i) {
      var date = new Date(time);
      this.timestamps.push(date);
      time.add(1, 'hours');
    }
  }
};

function WoWSummaryRow() {
  this.metricName = "N/A";
  this.timeGranularity = "HOURS";
  this.data = [];
}

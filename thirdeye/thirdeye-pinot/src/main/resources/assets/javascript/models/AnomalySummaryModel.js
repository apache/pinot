function AnomalySummaryModel() {
  this.startTime = moment().subtract(7, "days");
  this.endTime = moment();
  this.timestamps = [];
  this.anomalySummary = [];
}

AnomalySummaryModel.prototype = {

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
    // TODO: fetch relevant data from backend and update this.anomalySummary
  },

  buildSampleData: function() {
    var row1 = new AnomalySummaryRow();
    row1.metricName = "metricA";
    row1.data = [0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0];

    var row2 = new AnomalySummaryRow();
    row2.metricName = "metricB";
    row2.data = [0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0];

    var row3 = new AnomalySummaryRow();
    row3.metricName = "metricC";
    row3.data = [0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0];

    this.anomalySummary = [];
    this.anomalySummary.push(row1);
    this.anomalySummary.push(row2);
    this.anomalySummary.push(row3);

    this.timestamps = [];
    var time = moment(this.endTime).subtract(24, 'hours');
    for (var i = 0; i < 24; ++i) {
      var date = new Date(time);
      this.timestamps.push(date);
      time.add(1, 'hours');
    }
  }
};

function AnomalySummaryRow() {
  this.metricName = "N/A";
  this.timeGranularity = "HOURS";
  this.data = [];
}

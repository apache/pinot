function AnomalySummaryModel() {
  this.timeRangeLabels = [ "Last 6 Hours", "Last 24 Hours", "Last 48 Hours", "Last Week", "Last Month" ];
  this.anomalySummaryList = [];
}

AnomalySummaryModel.prototype = {

  init : function(params) {
    this.buildSampleData();
  },

  rebuild : function() {
    // TODO: fetch relevant data from backend and update this.anomalySummary
  },

  buildSampleData : function() {
    var row1 = new AnomalySummaryRow();
    row1.metricName = "metricA";
    row1.data = [ {resolved:3, open:5}, {resolved:3, open:5}, {resolved:3, open:5}, {resolved:3, open:5}, {resolved:3, open:5} ];

    var row2 = new AnomalySummaryRow();
    row2.metricName = "metricB";
    row2.data = [ {resolved:3, open:5}, {resolved:3, open:5}, {resolved:3, open:5}, {resolved:3, open:5}, {resolved:3, open:5} ];

    var row3 = new AnomalySummaryRow();
    row3.metricName = "metricC";
    row3.data = [ {resolved:3, open:5}, {resolved:3, open:5}, {resolved:3, open:5}, {resolved:3, open:5}, {resolved:3, open:5} ];

    this.anomalySummaryList = [];
    this.anomalySummaryList.push(row1);
    this.anomalySummaryList.push(row2);
    this.anomalySummaryList.push(row3);

  }
};

function AnomalySummaryRow() {
  this.metricName = "N/A";
  this.timeGranularity = "HOURS";
  this.data = [];
}

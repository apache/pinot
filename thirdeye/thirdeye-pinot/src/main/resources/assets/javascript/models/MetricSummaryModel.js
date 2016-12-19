function MetricSummaryModel() {
  this.timeRangeLabels = [ "Last 6 Hours", "Last 24 Hours", "Last 48 Hours", "Last Week", "Last Month" ];
  this.metrics =["Metric A", "Metric A", "Metric A", "Metric A", "Metric A", "Metric A", "Metric A", "Metric A" ];
  this.metricSummaryList = [];
}

MetricSummaryModel.prototype = {

  init : function(params) {
    this.buildSampleData();
  },

  rebuild : function() {
    // TODO: fetch relevant data from backend and update this.metricSummary
  },

  buildSampleData : function() {
    var row1 = new MetricSummaryRow();
    row1.metricName = "metricA";
    row1.data = [ {anomalies:3, wow:-20}, {resolved:3, open:5}, {resolved:3, open:5}, {resolved:3, open:5}, {resolved:3, open:5} ];

    var row2 = new MetricSummaryRow();
    row2.metricName = "metricB";
    row2.data = [ {resolved:3, open:0}, {resolved:3, open:5}, {resolved:3, open:5}, {resolved:3, open:5}, {resolved:3, open:5} ];

    var row3 = new MetricSummaryRow();
    row3.metricName = "metricC";
    row3.data = [ {resolved:0, open:0}, {resolved:3, open:0}, {resolved:5, open:0}, {resolved:3, open:5}, {resolved:3, open:5} ];

    this.metricSummaryList = [];
    this.metricSummaryList.push(row1);
    this.metricSummaryList.push(row2);
    this.metricSummaryList.push(row3);

  }
};

function MetricSummaryRow() {
  this.metricName = "N/A";
  this.timeGranularity = "HOURS";
  this.data = [];
}

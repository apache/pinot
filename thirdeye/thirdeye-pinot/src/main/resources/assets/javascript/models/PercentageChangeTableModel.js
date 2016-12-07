function PercentageChangeTableModel() {
  this.metricName = "MyMetric";
  this.dashboardName = "100 Most Recent Anomalies";
  this.startTime = moment().subtract(7, "days");
  this.endTime = moment();
  this.mode = "AnomalySummary";
  //this.anomalySummaryResult = { {"metricA": [0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0]},{"metricB": [0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0]},{"metricC": [0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0]}}
  this.wowMetricTable ={};
  this.wowMetricDimensionTable ={};
  this.showDetailsChecked = false;
  this.showCumulativeChecked = false;
  this.params;
}

PercentageChangeTableModel.prototype = {

  init : function(params) {
    this.params = params;
  },
  rebuild : function() {
  // TODO: fetch relevant data from backend
  }

}

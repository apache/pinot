function PercentageChangeTableModel() {
  this.metricName = "MyMetric";
  this.dashboardName = "100 Most Recent Anomalies";
  this.startTime = moment().subtract(7, "days");
  this.endTime = moment();
  this.mode = "AnomalySummary";
  //this.anomalySummaryResult = { {"metricA": [0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0]},{"metricB": [0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0]},{"metricC": [0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0]}}
  this.wowMetricTable ={};
  this.wowMetricDimensionTable ={};
}

PercentageChangeTableModel.prototype = {

  init : function(params) {
    if (params.dashboardName) {
      this.dashboardName = params.dashboardName;
    }
    if (params.startTime) {
      this.startTime = params.startTime;
    }
    if (params.dashboardName) {
      this.endTime = params.endTime;
    }
    if (params.dashboardName) {
      this.dashboardViewMode = params.dashboardViewMode;
    }
    console.log("Changed dashboardName to " + params);
  },
  rebuild : function() {
  // TODO: fetch relevant data from backend
    console.log("Changed dashboardName to " + this.dashboardName);
  }

}

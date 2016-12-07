function AnomalySummaryModel() {
  this.metricName = "MyMetric"
  this.dashboardName = "100 Most Recent Anomalies";
  this.startTime = moment().subtract(7, "days");
  this.endTime = moment();
  this.mode = "AnomalySummary";
  this.anomalySummary = { "rows": [
    {"metricName":"metricA", "data": [0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0]},
    {"metricName":"metricB", "data": [0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0]},
    {"metricName":"metricC", "data": [0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0,0,1,2,0,0,0]}]
  };
}

AnomalySummaryModel.prototype = {

  init : function(params) {
    if (params.startTime) {
      this.startTime = params.startTime;
    }
    if (params.endTime) {
      this.endTime = params.endTime;
    }
  },

  rebuild : function() {
    // TODO: fetch relevant data from backend and update this.anomalySummary
  }

};

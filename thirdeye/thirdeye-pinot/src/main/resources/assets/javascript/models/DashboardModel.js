function DashboardModel() {
  this.dashboardName = "100 Most Recent Anomalies";
  this.mode = "AnomalySummary";
}

DashboardModel.prototype = {

  update : function(params) {
    if (params.dashboardName) {
      this.dashboardName = params.dashboardName;
    }
    if (params.mode) {
      this.mode = params.mode;
    }
  },

};

function DashboardModel(params) {
  this.dashboardName = "100 Most Recent Anomalies";
  this.startTime = moment().subtract(7, "days");
  this.endTime = moment();
  this.mode = "AnomalySummary";
  this.hashParams = params;
}

DashboardModel.prototype = {

  init: function (params) {
    this.hashParams = params;
    if (params.dashboardName) {
      this.dashboardName = params.dashboardName;
    }
    if (params.startTime) {
      this.startTime = params.startTime;
    }
    if (params.endTime) {
      this.endTime = params.endTime;
    }
    if (params.dashboardViewMode) {
      this.dashboardViewMode = params.dashboardViewMode;
    }
    console.log("Changed dashboardName to " + params);
  },

  update: function (params) {
    console.log("Changed dashboardName to " + this.dashboardName);
  }

};

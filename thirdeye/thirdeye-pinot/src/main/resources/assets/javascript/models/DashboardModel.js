function DashboardModel() {
  this.dashboardName="100 Recent Anomalies";
  this.dashboardId;
  this.mode = "AnomalySummary";
}

DashboardModel.prototype = {

  update: function (params) {
    if (params) {
      if (params.dashboardName) {
        this.dashboardName = params.dashboardName;
      }
      if (params.dashboardId) {
        this.dashboardId = params.dashboardId;
      }
      if (params.mode) {
        this.mode = params.mode;
      }
    }
  },

};

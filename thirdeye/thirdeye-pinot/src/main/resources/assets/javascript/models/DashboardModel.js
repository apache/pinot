function DashboardModel() {
  this.dashboardName = null; // replace this with 100 Recent Anomalies
  this.summaryDashboardId = null;
  this.mode = constants.DASHBOARD_MODE_ANOMALY_SUMMARY;
}

DashboardModel.prototype = {

  update: function (params) {
    if (params) {
      if (params.dashboardName) {
        this.dashboardName = params.dashboardName;
      }
      if (params.summaryDashboardId) {
        this.summaryDashboardId = params.summaryDashboardId;
      }
      if (params.mode) {
        this.mode = params.mode;
      }
    }
  },

};

function DashboardModel() {
  this.dashboardName;
  this.mode = "AnomalySummary";
  this.startTime = moment().subtract("7", "days");
  this.endTime = moment();

}

DashboardModel.prototype = {

  update : function(params) {
    if (params.dashboardName) {
      this.dashboardName = params.dashboardName;
    }
    if (params.mode) {
      this.mode = params.mode;
    }
    if (params.startTime) {
      this.startTime = params.startTime;
    }
    if (params.endTime) {
      this.endTime = params.endTime;
    }
  },

};

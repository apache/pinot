function DashboardModel(params) {
  this.dashboardName;
  this.mode = "AnomalySummary";
  this.hashParams = params;
}

DashboardModel.prototype = {

  init: function (params) {
    this.hashParams = params;
    if (params.dashboardName) {
      this.dashboardName = params.dashboardName;
    }
    // if (params.startTime) {
    //   this.startTime = params.startTime;
    // }
    // if (params.endTime) {
    //   this.endTime = params.endTime;
    // }
    // if (params.dashboardViewMode) {
    //   this.dashboardViewMode = params.dashboardViewMode;
    // }
  },

  update: function (params) {
    this.hashParams = params;
    if (params.dashboardName) {
      this.dashboardName = params.dashboardName;
    }
  },

  setStartTime: function(startTime) {
    this.hashParams.startTime = startTime;
  },

  getStartTime: function() {
    return this.hashParams.startTime;
  },

  setEndTime: function(endTime) {
    this.hashParams.endTime = endTime;
  },

  getEndTime: function() {
    return this.hashParams.endTime;
  }

};

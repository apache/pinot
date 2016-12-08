function DashboardModel(params) {
  this.dashboardName = "100 Most Recent Anomalies";
  this.mode = "AnomalySummary";
  this.hashParams = params;

  // TODO: fetch this from backend
  this.dashboards = [ {
    value : 'Foo Dashboard',
    data : '1'
  }, {
    value : 'Bar Dashboard',
    data : '2'
  } ];
}

DashboardModel.prototype = {

  init: function (params) {
    this.hashParams = params;
    // if (params.dashboardName) {
    //   this.dashboardName = params.dashboardName;
    // }
    // if (params.startTime) {
    //   this.startTime = params.startTime;
    // }
    // if (params.endTime) {
    //   this.endTime = params.endTime;
    // }
    // if (params.dashboardViewMode) {
    //   this.dashboardViewMode = params.dashboardViewMode;
    // }
    console.log("Changed dashboardName to " + params);
  },

  update: function (params) {
    console.log("Changed dashboardName to " + this.dashboardName);
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

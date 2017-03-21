function AnomalySummaryModel() {
  this.dashboardName = null;
  this.summaryDashboardId = null
  this.previousDashboardName = null;

  this.timeRangeLabels = [ "Last 6 Hours", "Last 24 Hours", "Last 48 Hours", "Last Week", "Last Month" ];
  this.timeRanges = ['6_HOURS', '24_HOURS', '48_HOURS', '7_DAYS', '30_DAYS'];
  this.metricToAnomalySummaryListMap = {};

  this.renderViewEvent = new Event();
}

AnomalySummaryModel.prototype = {

  reset : function() {

  },
  setParams : function() {
    var params = HASH_SERVICE.getParams();
    if (params != undefined) {
      if (params[HASH_PARAMS.DASHBOARD_DASHBOARD_NAME] != undefined) {
        this.previousDashboardName = this.dashboardName;
        this.dashboardName = params[HASH_PARAMS.DASHBOARD_DASHBOARD_NAME];
        this.summaryDashboardId = params[HASH_PARAMS.DASHBOARD_SUMMARY_DASHBOARD_ID];
      }
    }
  },

  rebuild : function() {
    if (this.dashboardName != null) {
      if (this.previousDashboardName != this.dashboardName) {
        dataService.fetchAnomalySummary(this.dashboardName, this.timeRanges, this.updateModelAndNotifyView.bind(this));
      } else {
        this.updateModelAndNotifyView(this.metricToAnomalySummaryListMap);
      }
    }
  },
  updateModelAndNotifyView : function(metricToAnomalySummaryListMap) {
    this.metricToAnomalySummaryListMap = metricToAnomalySummaryListMap;
    this.renderViewEvent.notify();
  }
};


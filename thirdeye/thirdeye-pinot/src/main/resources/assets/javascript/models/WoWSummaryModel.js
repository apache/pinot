function WoWSummaryModel() {
  this.dashboardName = null;
  this.summaryDashboardId = null
  this.previousDashboardName = null;

  this.timeRangeLabels = [ "Most Recent Hour", "Today", "Yesterday", "Last 7 Days" ];
  this.wowSummary = null;

  this.renderViewEvent = new Event();
}

WoWSummaryModel.prototype = {

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
        dataService.fetchWowSummary(this.dashboardName, this.timeRangeLabels, this.updateModelAndNotifyView.bind(this));
      } else {
        this.updateModelAndNotifyView(this.wowSummary)
      }
    }
  },
  updateModelAndNotifyView : function(wowSummary) {
    this.wowSummary = wowSummary;
    this.renderViewEvent.notify();
  }
};


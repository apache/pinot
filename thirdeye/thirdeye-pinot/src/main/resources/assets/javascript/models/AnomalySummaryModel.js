function AnomalySummaryModel() {
  this.timeRangeLabels = [ "Last 6 Hours", "Last 24 Hours", "Last 48 Hours", "Last Week", "Last Month" ];
  this.timeRanges = ['6_HOURS', '24_HOURS', '48_HOURS', '7_DAYS', '30_DAYS'];
  this.metricToAnomalySummaryListMap = {};

  this.renderViewEvent = new Event();
}

AnomalySummaryModel.prototype = {

  reset : function() {
    this.metricToAnomalySummaryListMap = {};
  },
  setParams : function(params) {
    console.log("Set params");
    if (params != undefined) {
      console.log('params');
      if (params['dashboardName'] != undefined) {
        console.log('dashboard');
        this.dashboardName = params['dashboardName'];
      }
    }
  },

  rebuild : function() {
    if (this.dashboardName != null) {
      dataService.fetchAnomalySummary(this.dashboardName, this.timeRanges, this.updateModelAndNotifyView.bind(this));
    }
  },
  updateModelAndNotifyView : function(metricToAnomalySummaryListMap) {
    console.log('Results');
    console.log(metricToAnomalySummaryListMap);
    this.metricToAnomalySummaryListMap = metricToAnomalySummaryListMap;
    this.renderViewEvent.notify();
  }
};


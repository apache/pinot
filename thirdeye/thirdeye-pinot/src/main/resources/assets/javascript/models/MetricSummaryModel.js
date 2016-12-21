function MetricSummaryModel() {
  this.dashboardName = null;
  this.timeRange = "24_HOURS";
  this.metricSummaryList = [];

  this.renderViewEvent = new Event();
}

MetricSummaryModel.prototype = {

  reset : function() {
    this.metricSummaryList = [];
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
      dataService.fetchMetricSummary(this.dashboardName, this.timeRange, this.updateModelAndNotifyView.bind(this));
    }
  },
  updateModelAndNotifyView : function(metricSummaryList) {
    console.log('Results');
    console.log(metricSummaryList);
    this.metricSummaryList = metricSummaryList;
    this.renderViewEvent.notify();
  }
};


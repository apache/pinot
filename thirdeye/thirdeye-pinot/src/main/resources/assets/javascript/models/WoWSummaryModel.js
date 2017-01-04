function WoWSummaryModel() {
  this.dashboardName = null;
  this.dashboardId = null
  this.previousDashboardName = null;

  this.timeRangeLabels = [ "Most Recent Hour", "Today", "Yesterday", "Last 7 Days" ];
  this.wowSummary = null;

  this.renderViewEvent = new Event();
}

WoWSummaryModel.prototype = {

  reset : function() {

  },
  setParams : function(params) {
    console.log("Set params for WOW");
    if (params != undefined) {
      console.log('params');
      if (params['dashboardName'] != undefined) {
        console.log('dashboard');
        this.previousDashboardName = this.dashboardName;
        this.dashboardName = params['dashboardName'];
        this.dashboardId = params['dashboardId'];
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
    console.log('Results');
    console.log(wowSummary);
    this.wowSummary = wowSummary;
    this.renderViewEvent.notify();
  },
  buildSampleData : function() {
    this.wowSummaryList = [];
    for (i = 0; i < 3; i++) {
      var row = new WoWSummaryRow();
      row.metricName = "metric" + i;
      row.data = [ {
        baseline : 1000,
        current : 2000,
        percentChange : 50,
        startTime : moment().startOf('hour'),
        endTime : moment().startOf('hour').subtract('1', 'hour')
      }, {
        baseline : 1000,
        current : 2000,
        percentChange : -50,
        startTime : moment().startOf('hour'),
        endTime : moment().startOf('day')
      }, {
        baseline : 1000,
        current : 2000,
        percentChange : -10,
        startTime : moment().startOf('day'),
        endTime : moment().startOf('day').subtract('1', 'day')
      }, {
        baseline : 1000,
        current : 2000,
        percentChange : 10,
        startTime : moment().startOf('day'),
        endTime : moment().startOf('day').subtract('7', 'day')
      } ];
      this.wowSummaryList.push(row);
    }
    this.updateModelAndNotifyView(this.wowSummaryList);

  }
};

function WoWSummaryRow() {
  this.metricName = "N/A";
  this.data = [];
}

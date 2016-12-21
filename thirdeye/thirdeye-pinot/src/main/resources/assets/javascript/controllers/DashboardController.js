function DashboardController(parentController) {
  this.parentController = parentController;
  this.dashboardModel = new DashboardModel();
  this.dashboardView = new DashboardView(this.dashboardModel);

  this.metricSummaryController = new MetricSummaryController(this);
  this.anomalySummaryController = new AnomalySummaryController(this);
  this.wowSummaryController = new WoWSummaryController(this);

  this.dashboardView.tabClickEvent.attach(this.onSubTabSelectionEventHandler.bind(this));
  this.dashboardView.onDashboardSelectionEvent.attach(this.onDashboardSelectionEventHandler.bind(this));
}

DashboardController.prototype = {
  init : function(hashParams) {
    this.dashboardModel.update(HASH_SERVICE.getParams());
    this.dashboardView.init();
  },

  handleAppEvent : function(args) {
    console.log("DashboardController.handleAppEvent");
    tabName = HASH_SERVICE.get("tab");

    console.log("tabName:" + tabName);
    var childController;
    if (tabName.startsWith("dashboard_metric-summary-tab")) {
      this.dashboardModel.tabSelected = "dashboard_metric-summary-tab";
      childController = this.metricSummaryController;
    } else if (tabName.startsWith("dashboard_anomaly-summary-tab")) {
      this.dashboardModel.tabSelected = "dashboard_anomaly-summary-tab";
      childController = this.anomalySummaryController;
    } else if (tabName.startsWith("dashboard_wow-summary-tab")) {
      this.dashboardModel.tabSelected = "dashboard_wow-summary-tab";
      childController = this.wowSummaryController;
    } else {
      this.dashboardModel.tabSelected = "dashboard_anomaly-summary-tab";
      childController = this.anomalySummaryController;
    }
    this.dashboardView.render();
    console.log('Sending to child controller');
    console.log(childController);
    childController.handleAppEvent(args);

  },

  onSubTabSelectionEventHandler : function(sender, args) {
    var params = this.dashboardModel.hashParams;
    args.targetTab = args.targetTab.replace("#", "");
    HASH_SERVICE.set("tab", args.targetTab);
    this.handleAppEvent();
  },

  hideDataRangePickerEventHandler : function(sender, args) {
    var dataRangePicker = args.dataRangePicker;
    if (!this.dashboardModel.startTime.isSame(dataRangePicker.startDate) || !this.dashboardModel.endTime.isSame(dataRangePicker.endDate)) {
      // Copy date range to local model for checking if new date range needs
      // update
      this.dashboardModel.startTime = dataRangePicker.startDate;
      this.dashboardModel.endTime = dataRangePicker.endDate;
      this.handleAppEvent();
    }

  },

  onDashboardSelectionEventHandler : function(sender, args) {
    HASH_SERVICE.update(args);
    this.dashboardModel.dashboardName = args.dashboardName;
    this.dashboardModel.dashboardId = args.dashboardId;
    console.log(HASH_SERVICE.getParams());
    this.handleAppEvent(args);
  }

};

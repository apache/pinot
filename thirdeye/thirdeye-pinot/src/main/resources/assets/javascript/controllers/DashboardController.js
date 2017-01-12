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

  handleAppEvent : function() {
    console.log("DashboardController.handleAppEvent");
    //tabName = HASH_SERVICE.get("tab");
    mode = HASH_SERVICE.get(HASH_PARAMS.DASHBOARD_MODE);
    console.log("tabName:" + tabName + " mode: " + mode);
    var childController;
    /*if (tabName.startsWith("dashboard_metric-summary-tab")) {
      this.dashboardModel.tabSelected = "dashboard_metric-summary-tab";
      this.dashboardModel.mode = 'MetricSummary';
      childController = this.metricSummaryController;
    } else if (tabName.startsWith("dashboard_anomaly-summary-tab")) {
      this.dashboardModel.tabSelected = "dashboard_anomaly-summary-tab";
      this.dashboardModel.mode = 'AnomalySummary';
      childController = this.anomalySummaryController;
    } else if (tabName.startsWith("dashboard_wow-summary-tab")) {
      this.dashboardModel.tabSelected = "dashboard_wow-summary-tab";
      this.dashboardModel.mode = 'WowSummary';
      childController = this.wowSummaryController;
    } else {
      this.dashboardModel.tabSelected = "dashboard_metric-summary-tab";
      childController = this.metricSummaryController;
    }*/
    if (mode == constants.DASHBOARD_MODE_METRIC_SUMMARY) {
      this.dashboardModel.tabSelected = "dashboard_metric-summary-tab";
      //this.dashboardModel.mode = 'MetricSummary';
      childController = this.metricSummaryController;
    } else if (mode == constants.DASHBOARD_MODE_ANOMALY_SUMMARY) {
      this.dashboardModel.tabSelected = "dashboard_anomaly-summary-tab";
      //this.dashboardModel.mode = 'AnomalySummary';
      childController = this.anomalySummaryController;
    } else if (mode == constants.DASHBOARD_MODE_WOW_SUMMARY) {
      this.dashboardModel.tabSelected = "dashboard_wow-summary-tab";
      //this.dashboardModel.mode = 'WowSummary';
      childController = this.wowSummaryController;
    }
    this.dashboardModel.dashboardName = HASH_SERVICE.get(HASH_PARAMS.DASHBOARD_DASHBOARD_NAME);
    this.dashboardModel.summaryDashboardId = HASH_SERVICE.get(HASH_PARAMS.DASHBOARD_SUMMARY_DASHBOARD_ID);
    this.dashboardView.render();
    console.log('Sending to child controller');
    console.log(childController);
    //var args = {
    //    dashboardName : this.dashboardModel.dashboardName,
    //    dashboardId : this.dashboardModel.dashboardId,
    //    dashboardMode : this.dashboardModel.mode
    //};
    childController.handleAppEvent();
  },

  onSubTabSelectionEventHandler : function(sender, args) {
    console.log('onSubTabSelectionEventHandler');
    console.log(args);
    args.targetTab = args.targetTab.replace("#", "");
    var mode = constants.DASHBOARD_MODE_ANOMALY_SUMMARY;
    switch (args.targetTab) {
    case 'dashboard_metric-summary-tab':
      mode = constants.DASHBOARD_MODE_METRIC_SUMMARY;
      break;
    case 'dashboard_anomaly-summary-tab':
      mode = constants.DASHBOARD_MODE_ANOMALY_SUMMARY;
      break;
    case 'dashboard_wow-summary-tab':
      mode = constants.DASHBOARD_MODE_WOW_SUMMARY;
      break;
    }
    this.dashboardModel.mode = mode;
    HASH_SERVICE.set(HASH_PARAMS.DASHBOARD_MODE, mode);
    HASH_SERVICE.route();
  },

  onDashboardSelectionEventHandler : function(sender, args) {
    console.log('dashboard selection event handler');
    HASH_SERVICE.update(args);
    //this.dashboardModel.dashboardName = args.dashboardName;
    //this.dashboardModel.dashboardId = args.dashboardId;
    //this.handleAppEvent();
    HASH_SERVICE.route();
  }

};

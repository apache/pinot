function DashboardController(parentController) {
  this.parentController = parentController;
  this.dashboardModel = new DashboardModel();
  this.dashboardView = new DashboardView(this.dashboardModel);

  this.metricSummaryController = new MetricSummaryController(this);
  this.anomalySummaryController = new AnomalySummaryController(this);
  this.wowSummaryController = new WoWSummaryController(this);
  HASH_SERVICE.registerController('metricSummary', this.metricSummaryController);
  HASH_SERVICE.registerController('anomalySummary', this.anomalySummaryController);
  HASH_SERVICE.registerController('wowSummary', this.wowSummaryController);

  this.dashboardView.tabClickEvent.attach(this.onSubTabSelectionEventHandler.bind(this));
  this.dashboardView.onDashboardSelectionEvent.attach(this.onDashboardSelectionEventHandler.bind(this));
}

DashboardController.prototype = {
  init : function(hashParams) {
    this.dashboardModel.update(HASH_SERVICE.getParams());
    this.dashboardView.init();
  },

  handleAppEvent : function() {
    const mode = HASH_SERVICE.get(HASH_PARAMS.DASHBOARD_MODE);
    let controllerName;
    switch(mode){
      case constants.DASHBOARD_MODE_METRIC_SUMMARY:
        this.dashboardModel.tabSelected = 'dashboard_metric-summary-tab';
        controllerName = 'metricSummary';
        break;
      case constants.DASHBOARD_MODE_WOW_SUMMARY:
        this.dashboardModel.tabSelected = 'dashboard_wow-summary-tab';
        controllerName = 'wowSummary';
        break;
      default:
        this.dashboardModel.tabSelected = 'dashboard_anomaly-summary-tab';
        controllerName = 'anomalySummary';
    }
    this.dashboardModel.dashboardName = HASH_SERVICE.get(HASH_PARAMS.DASHBOARD_DASHBOARD_NAME);
    this.dashboardModel.summaryDashboardId = HASH_SERVICE.get(HASH_PARAMS.DASHBOARD_SUMMARY_DASHBOARD_ID);
    this.dashboardView.render();
    HASH_SERVICE.routeTo(controllerName);
  },

  onSubTabSelectionEventHandler : function(sender, args) {
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
    HASH_SERVICE.refreshWindowHashForRouting('dashboard');
    HASH_SERVICE.routeTo('dashboard');
  },

  onDashboardSelectionEventHandler : function(sender, args) {
    HASH_SERVICE.update(args);
    HASH_SERVICE.refreshWindowHashForRouting('dashboard');
    HASH_SERVICE.routeTo('dashboard');
  }

};

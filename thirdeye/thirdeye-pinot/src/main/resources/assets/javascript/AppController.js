function AppController() {
  // CONTROLLERs
  this.dashboardController = new DashboardController(this);
  this.anomalyResultController = new AnomalyResultController(this);
  this.analysisController = new AnalysisController(this);
  HASH_SERVICE.registerController('dashboard', this.dashboardController);
  HASH_SERVICE.registerController('anomalies', this.anomalyResultController);
  HASH_SERVICE.registerController('analysis', this.analysisController);

  this.appModel = new AppModel();
  this.appView = new AppView(this.appModel);

  // add listeners
  this.appView.tabClickEvent.attach(this.onTabClickEventHandler.bind(this));
}

AppController.prototype = {
  init : function() {
    console.log("App controller init ");
    this.appView.init();
    this.compileTemplates();
    HASH_SERVICE.setHashParamsFromUrl();
  },

  handleAppEvent : function() {
    console.log('In AppController.handleAppEvent');
    tabName = HASH_SERVICE.get('tab');
    this.appModel.tabSelected = tabName;
    console.log('tabName:' + tabName);
    var controllerName = 'anomalies';
    if (tabName.startsWith('dashboard')) {
      this.appModel.tabSelected = 'dashboard';
      controllerName = 'dashboard';
    } else if (tabName.startsWith('anomalies')) {
      this.appModel.tabSelected = 'anomalies';
      controllerName = 'anomalies';
    } else if (tabName.startsWith('analysis')) {
      this.appModel.tabSelected = 'analysis';
      controllerName = 'analysis';
    }
    this.appView.render();
    HASH_SERVICE.routeTo(controllerName);
  },

  compileTemplates : function() {
    // compile templates
    var ingraph_metric_config_template = $("#ingraph-metric-config-template").html();
    ingraph_metric_config_template_compiled = Handlebars.compile(ingraph_metric_config_template);

    var metric_config_template = $("#metric-config-template").html();
    metric_config_template_compiled = Handlebars.compile(metric_config_template);

    var job_info_template = $("#job-info-template").html();
    job_info_template_compiled = Handlebars.compile(job_info_template);
  },

  onTabClickEventHandler : function(sender, args) {
    console.log('App controller onTabCLickEventHandler');
    console.log("targetTab:" + args.targetTab);
    console.log("previousTab:" + args.previousTab);
    if (args.targetTab != args.previousTab) {
      args.targetTab = args.targetTab.replace("#", "");
      HASH_SERVICE.set("tab", args.targetTab);
      HASH_SERVICE.routeTo('app');
    }
  }
};

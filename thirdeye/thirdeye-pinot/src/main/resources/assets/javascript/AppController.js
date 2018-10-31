function AppController() {
  // CONTROLLERs
  this.anomalyResultController = new AnomalyResultController(this);
  this.analysisController = new AnalysisController(this);
  this.investigateController = new InvestigateController(this);
  HASH_SERVICE.registerController('anomalies', this.anomalyResultController);
  HASH_SERVICE.registerController('analysis', this.analysisController);
  HASH_SERVICE.registerController('investigate', this.investigateController);

  this.appModel = new AppModel();
  this.appView = new AppView(this.appModel);

  // add listeners
  this.appView.tabClickEvent.attach(this.onTabClickEventHandler.bind(this));
}

AppController.prototype = {
  init : function() {
    this.appView.init();
    this.compileTemplates();
    HASH_SERVICE.setHashParamsFromUrl();
  },

  handleAppEvent : function() {
    let tabName = HASH_SERVICE.get('tab');
    this.appModel.tabSelected = tabName;
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
    } else if (tabName.startsWith('investigate')) {
      this.appModel.tabSelected = 'investigate';
      controllerName = 'investigate';
    }
    this.appView.render();
    HASH_SERVICE.routeTo(controllerName);
  },

  compileTemplates : function() {
    // compile templates

    var metric_config_template = $("#metric-config-template").html();
    Handlebars.compile(metric_config_template);

    var job_info_template = $("#job-info-template").html();
    Handlebars.compile(job_info_template);
  },

  onTabClickEventHandler : function(sender, args) {
    if (args.targetTab != args.previousTab) {
      args.targetTab = args.targetTab.replace("#", "");
      HASH_SERVICE.set("tab", args.targetTab);
      HASH_SERVICE.refreshWindowHashForRouting('app');
      HASH_SERVICE.routeTo('app');
    }
  }
};

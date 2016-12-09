function AppController() {
  // CONTROLLERs
  this.dashboardController = new DashboardController(this);
  this.anomalyResultController = new AnomalyResultController(this);
  this.analysisController = new AnalysisController(this);
  this.appModel = new AppModel();
  this.appView = new AppView(this.appModel);

  // add listeners
  this.appView.tabClickEvent.attach(this.onTabClickEventHandler.bind(this));
}

AppController.prototype = {
  init : function() {
    console.log("init called");
    this.appView.init();
    // init page routing here
    this.compileTemplates();
    this.handleAppEvent();
  },

  handleAppEvent : function() {
    console.log("In AppController.handleAppEvent");
    tabName = HASH_SERVICE.get("tab");
    this.appModel.tabSelected = tabName;
    console.log("tabName:" + tabName);
    var childController;
    if (tabName.startsWith("dashboard")) {
      this.appModel.tabSelected = "dashboard";
      childController = this.dashboardController;
    } else if (tabName.startsWith("anomalies")) {
      this.appModel.tabSelected = "anomalies";
      childController = this.anomalyResultController;
    } else if (tabName.startsWith("analysis")) {
      this.appModel.tabSelected = "analysis";
      childController = this.analysisController;
    } else {
      this.appModel.tabSelected = "dashboard";
      childController = this.dashboardController;
    }
    this.appView.render();
    childController.handleAppEvent();
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

  /**
   * Place holder that gets invoked before every call. parse the hash
   */
  parseHash : function(ctx, next) {
    console.log("START: parse hash" + ctx.path);
    // TODO: update ctx.state.hashParams (String) to this.appModel.hashParams
    // (Map)
    ctx.state.hashParams = {};
    ctx.state.hashParams.dashboardName = "New Dashboard";
    ctx.hashParams = this.appModel.hashParams;

    next();
    console.log("END: parse hash" + ctx.path);

  },
  onTabClickEventHandler : function(sender, args) {
    console.log("targetTab:" + args.targetTab);
    console.log("previousTab:" + args.previousTab);
    if (args.targetTab != args.previousTab) {
      args.targetTab = args.targetTab.replace("#", "");
      HASH_SERVICE.set("tab", args.targetTab);
      this.handleAppEvent();
    }
  }
};

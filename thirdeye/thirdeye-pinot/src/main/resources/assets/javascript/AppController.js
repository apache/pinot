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
    this.setupRouting();
    this.handleURL();
  },

  handleURL : function() {
    console.log("window.location:" + window.location.hash);
    tab = "dashboard";
    if (window.location.hash) {
      splits = window.location.hash.split('/');
      if (splits.length > 1) {
        console.log("hash split[0]" + splits[1]);
        tab = splits[1];
      }
    }
    $("#main-tabs a[href='#" + tab + "']").click();
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

  setupRouting : function() {
    page.base("/thirdeye");
    page("/", this.parseHash, this.dashboardController.handleAppEvent.bind(this.dashboardController));
    page("/dashboard", this.parseHash, this.dashboardController.handleAppEvent.bind(this.dashboardController));
    page("/anomalies", this.parseHash, this.anomalyResultController.handleAppEvent.bind(this.anomalyResultController));
    page("/analysis", this.parseHash, this.analysisController.handleAppEvent.bind(this.analysisController));
    // page("/ingraph-metric-config", this.updateHistory,
    // showIngraphDatasetSelection);
    // page("/ingraph-dashboard-config", this.updateHistory,
    // listIngraphDashboardConfigs);
    // page("/metric-config", this.updateHistory, showMetricDatasetSelection);
    // page("/dataset-config", this.updateHistory, listDatasetConfigs);
    // page("/job-info", this.updateHistory, listJobs);
    // page("/entity-editor", this.updateHistory, renderConfigSelector);

    page.start({
      hashbang : true
    });
    // everytime hash changes, we should handle the new hash
    $(window).on('hashchange', this.handleURL);
  },
  /**
   * Place holder that gets invoked before every call. parse the hash
   */
  parseHash : function(ctx, next) {
    // parseHash
    ctx.state.hashParams = {};
    ctx.state.hashParams.dashboardName = "New Dashboard";
    next(ctx.state.hashParams);
  },
  onTabClickEventHandler : function(sender, args) {
    console.log("targetTab:" + args.targetTab);
    console.log("previousTab:" + args.previousTab);
    if (args.targetTab != args.previousTab) {
      args.targetTab = args.targetTab.replace("#", "");
      // page("/thirdeye/" + args.targetTab)
      if (args.targetTab == "dashboard") {
        this.dashboardController.handleAppEvent(args);
      } else if (args.targetTab == "anomalies") {
        this.anomalyResultController.handleAppEvent(args);
      } else if (args.targetTab == "analysis") {
        this.analysisController.handleAppEvent(args);
      }
    }
  }
};

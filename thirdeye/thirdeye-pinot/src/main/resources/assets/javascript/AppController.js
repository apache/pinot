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
    var dashboard_template = $("#dashboard-template").html();
    dashboard_template_compiled = Handlebars.compile(dashboard_template);

    var anomalies_template = $("#anomalies-template").html();
    anomalies_template_compiled = Handlebars.compile(anomalies_template);

    var analysis_template = $("#analysis-template").html();
    analysis_template_compiled = Handlebars.compile(analysis_template);

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
    // page("/anomalies", this.parseHash, this.anomalyResultController.handleAppEvent.bind(this.anomalyResultController));
    // page("/analysis", this.parseHash, this.analysisController.handleAppEvent.bind(this.analysisController));
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
    next();
  },
  onTabClickEventHandler : function(sender, args) { //targetTab, previousTab) {
    console.log("targetTab:" + args.targetTab);
    console.log("previousTab:" + args.previousTab);
    if (args.targetTab != args.previousTab) {
      args.targetTab = args.targetTab.replace("#", "");
      page("/thirdeye/" + args.targetTab)
    }
  },
}

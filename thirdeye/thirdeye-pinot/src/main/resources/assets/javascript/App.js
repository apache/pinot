function App() {
  // CONTROLLERs
  this.dashboardController = new DashboardController();
  this.anomalyResultController = new AnomalyResultController();

  // MODELs
  this.dashboardModel = new DashboardModel();
  this.analysisModel = new DashboardModel();
  this.anomalyResultModel = new AnomalyResultModel();

  // VIEWs (sub views will be managed by individual views)
  this.dashboardView = new DashboardView();
  this.anomalyResultView = new AnomalyResultView();
  this.analysisView = new AnalysisView();

}

App.prototype = {
  init : function() {
    console.log("init called");
    // init page routing here
    this.compileTemplates();
    this.setupListeners();
    this.setupRouting();
    this.handleURL();
    // Prepare
    var History = window.History; // Note: We are using a capital H instead of a
    // lower h
    if (!History.enabled) {
      // History.js is disabled for this browser.
      // This is because we can optionally choose to support HTML4 browsers or
      // not.
      return false;
    }

    // Bind to StateChange Event
    History.Adapter.bind(window, 'statechange', function() { // Note: We are
      // using
      // statechange
      // instead of
      // popstate
      var State = History.getState();
      //History.log(State.data, State.title, State.url);
      //this.handleURL();
    });

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
    var routes = {
      "/#dashboard" : this.dashboardView.render,
      "/#anomalies" : this.anomalyResultView.render,
      "/#analysis" : this.analysisView.render,
    };
    var router = Router(routes);
    // router.init();
    page.base("/thirdeye");
    page("/", this.dashboardView.render);
    page("/thirdeye", this.dashboardView.render);
    page("/dashboard", this.dashboardView.render);
    page("/anomalies", this.anomalyResultView.render);
    page("/analysis", this.analysisView.render);
    page("/ingraph-metric-config", showIngraphDatasetSelection);
    page("/ingraph-dashboard-config", listIngraphDashboardConfigs);
    page("/metric-config", showMetricDatasetSelection);
    page("/dataset-config", listDatasetConfigs);
    page("/job-info", listJobs);
    page("/entity-editor", renderConfigSelector);

    page.start({
      hashbang : true
    });
  },

  setupListeners : function() {
    var tabSelectionEventHandler = function(e) {
      var targetView = $(e.target).attr('href')
      //History.pushState(null, $(this).text(), targetView);
      console.log("targetView:" + targetView)
      targetView = targetView.replace("#", "");
      page("/thirdeye/" + targetView)
    }
    $('#main-tabs a[data-toggle="tab"]').on('shown.bs.tab', tabSelectionEventHandler);
    $('#admin-tabs a[data-toggle="tab"]').on('shown.bs.tab', tabSelectionEventHandler);
    $('#global-navbar a').on('shown.bs.tab', tabSelectionEventHandler);

  },
}
function DashboardController(parentController) {
  this.parentController = parentController;
  this.dashboardModel = new DashboardModel();
  this.dashboardView = new DashboardView(this.dashboardModel);
  console.log("initialized dashboard controller:" + this.dashboardView);

  this.anomalySummaryController = new AnomalySummaryController(this);
  this.woWSummaryController = new WoWSummaryController(this);

  this.dashboardView.tabClickEvent.attach(this.onTabClickEventHandler.bind(this));
}

DashboardController.prototype = {
  //TODO: figure out how to invoke this function from page callback
  handleAppEvent : function(ctx) {
    console.log("dashboardView: params from ctx" + ctx.state.hashParams);
    this.dashboardModel.init(ctx.state.hashParams);
    this.dashboardModel.update();
    this.dashboardView.init(ctx.state.hashParams);
    this.dashboardView.render();
    if (this.dashboardModel.mode == "AnomalySummary") {
      this.anomalySummaryController.handleAppEvent(ctx.state.hashParams)
    } else if (this.dashboardModel.mode == "WoWSummary") {
      this.woWSummaryController.handleAppEvent(ctx.state.hashParams)
    }

  },
  onDashboardInputChange : function() {

  },

  init : function() {

  },
  onTabClickEventHandler : function(sender, args) {
    var params = this.dashboardModel.hashParams;
    if (args == "#anomaly-summary-tab") {
      this.anomalySummaryController.handleAppEvent(params);
    } else if (args == "#wow-summary-tab") {
      this.woWSummaryController.handleAppEvent(params);
    }
  }

};

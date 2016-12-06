function DashboardController(parentController) {
  this.parentController = parentController;
  this.dashboardModel = new DashboardModel();
  this.dashboardView = new DashboardView(this.dashboardModel);
  console.log("initialized dashboard controller:" + this.dashboardView);

  this.anomalySummaryController = new AnomalySummaryController();
}

DashboardController.prototype = {
  //TODO: figure out how to invoke this function from page callback
  handleAppEvent : function(ctx) {
    console.log("dashboardView: params from ctx" + ctx.state.hashParams);
    this.dashboardModel.init(ctx.state.hashParams);
    this.dashboardModel.rebuild();
    this.dashboardView.render();
    if (this.dashboardModel.mode == "AnomalySummary") {
      this.anomalySummaryController.handleAppEvent(ctx.state.hashParams)
    }

  },
  onDashboardInputChange : function() {

  },

  init : function() {

  }

}
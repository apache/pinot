function TimeSeriesCompareController(parentController) {
  this.parentController = parentController;
  this.timeSeriesCompareModel= new TimeSeriesCompareModel();
  this.timeSeriesCompareView = new TimeSeriesCompareView(this.timeSeriesCompareModel);
  console.log("initialized TimeSeriesCompare controller:" + this.timeSeriesCompareView);
}

TimeSeriesCompareController.prototype = {
  handleAppEvent : function(ctx) {
    console.log("dashboardView: params from ctx" + ctx.state.hashParams);
    this.timeSeriesCompareModel.init(ctx.state.hashParams);
    this.timeSeriesCompareModel.update();
    this.timeSeriesCompareView.init();
    this.timeSeriesCompareView.render();
  }
};

function AnalysisController(parentController) {
  this.parentController = parentController;
  this.analysisModel = new AnalysisModel();
  this.analysisView = new AnalysisView();
  this.timeSeriesCompareController = new TimeSeriesCompareController(this);

}

AnalysisController.prototype = {
  handleAppEvent: function (ctx) {
    this.analysisModel.init(ctx.state.hashParams);
    this.analysisModel.update();
    this.analysisView.init();
    this.analysisView.render();
    this.timeSeriesCompareController.handleAppEvent(ctx);
  }
};

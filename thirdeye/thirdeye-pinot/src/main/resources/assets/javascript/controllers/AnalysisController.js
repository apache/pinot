function AnalysisController(parentController) {
  this.parentController = parentController;
  this.analysisModel = new AnalysisModel();
  this.analysisView = new AnalysisView();
}

AnalysisController.prototype = {
  handleAppEvent: function (ctx) {
    this.analysisModel.init(ctx.state.hashParams);
    this.analysisModel.update();
    this.analysisView.init();
    this.analysisView.render();
  }
};

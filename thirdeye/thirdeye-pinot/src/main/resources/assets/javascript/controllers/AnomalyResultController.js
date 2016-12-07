function AnomalyResultController(parentController) {
  this.parentController = parentController;
  this.anomalyResultModel = new AnomalyResultModel();
  this.anomalyResultView = new AnomalyResultView(this.anomalyResultModel);
}

AnomalyResultController.prototype = {
  handleAppEvent: function (ctx) {
    this.anomalyResultModel.init(ctx.state.hashParams);
    this.anomalyResultModel.update();
    this.anomalyResultView.init();
    this.anomalyResultView.render();
  }
};

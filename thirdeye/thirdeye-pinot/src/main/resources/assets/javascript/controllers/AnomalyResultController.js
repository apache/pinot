function AnomalyResultController(parentController) {
  this.parentController = parentController;
  this.anomalyResultModel = new AnomalyResultModel();
  this.anomalyResultView = new AnomalyResultView(this.anomalyResultModel);

  this.anomalyResultView.applyButtonEvent.attach(this.applyButtonEventHandler.bind(this));
  this.anomalyResultView.investigateButtonClickEvent.attach(this.investigateButtonClickEventHandler.bind(this));

  this.anomalyResultView.init();
}

AnomalyResultController.prototype = {
  handleAppEvent: function () {
    const params = HASH_SERVICE.getParams();
    const hasSameParams = this.anomalyResultModel.hasSameParams(params);
    if (hasSameParams) {
      this.anomalyResultView.render();
    } else {
      this.anomalyResultView.destroy();
      this.anomalyResultModel.reset();
      this.anomalyResultModel.setParams(params);
      this.anomalyResultModel.rebuild();
    }
  },
  applyButtonEventHandler: function(sender, args) {
    HASH_SERVICE.clear();
    HASH_SERVICE.set('tab', 'anomalies');
    HASH_SERVICE.update(args);
    HASH_SERVICE.refreshWindowHashForRouting('anomalies');
    HASH_SERVICE.routeTo('anomalies');
  },
  investigateButtonClickEventHandler: function (sender, args) {
    HASH_SERVICE.clear();
    HASH_SERVICE.set('tab', 'investigate');
    HASH_SERVICE.update(args);
    HASH_SERVICE.refreshWindowHashForRouting('investigate');
    HASH_SERVICE.routeTo('app');
    // Send this event and the args to parent controller, to route to AnalysisController
  },
};

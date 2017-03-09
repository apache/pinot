function AnalysisController(parentController) {
  this.parentController = parentController;
  this.analysisModel = new AnalysisModel();
  this.analysisView = new AnalysisView(this.analysisModel);
  this.timeSeriesCompareController = new TimeSeriesCompareController(this);

  // Event handlers
  this.analysisView.applyDataChangeEvent.attach(this.handleApplyAnalysisEvent.bind(this));
}

AnalysisController.prototype = {
  handleAppEvent: function () {
    let hashParams = HASH_SERVICE.getParams();
    if (hashParams.metricId) {
      HASH_SERVICE.refreshWindowHashForRouting('analysis');
      hashParams = HASH_SERVICE.getParams();
    }
    this.analysisModel.init(hashParams);
    this.analysisModel.update(hashParams);
    this.analysisView.init(hashParams);
    this.analysisView.render();
    this.timeSeriesCompareController.handleAppEvent(hashParams);
  },

  handleApplyAnalysisEvent: function (viewObject) {
    HASH_SERVICE.update(viewObject.viewParams);
    HASH_SERVICE.refreshWindowHashForRouting('analysis');
    this.timeSeriesCompareController.handleAppEvent(HASH_SERVICE.getParams());
  }
};


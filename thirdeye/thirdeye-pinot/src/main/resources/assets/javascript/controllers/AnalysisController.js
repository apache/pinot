function AnalysisController(parentController) {
  this.parentController = parentController;
  this.analysisModel = new AnalysisModel();
  this.analysisView = new AnalysisView(this.analysisModel);
  this.timeSeriesCompareController = new TimeSeriesCompareController(this);

  // Event handlers
  this.analysisView.applyDataChangeEvent.attach(this.handleApplyAnalysisEvent.bind(this));
}

AnalysisController.prototype = {
  handleAppEvent: function (hashParams) {
    this.analysisModel.init(hashParams);
    this.analysisModel.update(HASH_SERVICE.getParams());
    this.analysisView.init();
    this.analysisView.render();
    this.timeSeriesCompareController.handleAppEvent(hashParams);
   // this.dimensionTreeMapController.handleAppEvent(hashParams);
  },

  handleApplyAnalysisEvent: function (viewObject) {
    HASH_SERVICE.update(viewObject.viewParams);
    this.timeSeriesCompareController.handleAppEvent(HASH_SERVICE.getParams());
  }
}


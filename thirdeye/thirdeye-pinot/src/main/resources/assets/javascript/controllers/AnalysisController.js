function AnalysisController(parentController) {
  this.parentController = parentController;
  this.analysisModel = new AnalysisModel();
  this.analysisView = new AnalysisView(this.analysisModel);
  this.timeSeriesCompareController = new TimeSeriesCompareController(this);
  this.dimensionTreeMapController = new DimensionTreeMapController(this);

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
    this.dimensionTreeMapController.handleAppEvent(hashParams);
  },

  handleApplyAnalysisEvent: function (updatedAnalysisParams) {
    HASH_SERVICE.update(updatedAnalysisParams.viewParams);
    console.log("updated hash params in analysis controller ---> ");
    console.log(HASH_SERVICE.getParams());
    this.timeSeriesCompareController.handleAppEvent(HASH_SERVICE.getParams());
  }
}


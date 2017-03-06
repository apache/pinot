function InvestigateController(parentController) {
  this.parentController = parentController;
  this.investigateModel = new InvestigateModel();
  this.investigateView = new InvestigateView(this.investigateModel);
  // Event handlers
  // this.analysisView.applyDataChangeEvent.attach(this.handleApplyAnalysisEvent.bind(this));
}

InvestigateController.prototype = {
  handleAppEvent: function () {
    const hashParams = HASH_SERVICE.getParams();
    this.investigateModel.init(hashParams);
    this.investigateModel.update(hashParams);
    this.investigateView.init(hashParams);
    this.investigateView.render();
    // this.timeSeriesCompareController.handleAppEvent(hashParams);
  },

  handleApplyAnalysisEvent: function (viewObject) {
    // HASH_SERVICE.update(viewObject.viewParams);
    // HASH_SERVICE.refreshWindowHashForRouting('analysis');
    // this.timeSeriesCompareController.handleAppEvent(HASH_SERVICE.getParams());
  }
};


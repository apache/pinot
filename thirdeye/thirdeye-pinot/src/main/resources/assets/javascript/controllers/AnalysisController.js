function AnalysisController(parentController) {
  this.parentController = parentController;
  this.analysisModel = new AnalysisModel();
  this.analysisView = new AnalysisView(this.analysisModel);
  this.timeSeriesCompareController = new TimeSeriesCompareController(this);

  // Event handlers
  this.analysisView.searchEvent.attach(this.handleSearchEvent.bind(this));
  this.analysisView.applyDataChangeEvent.attach(this.handleApplyAnalysisEvent.bind(this));
}

AnalysisController.prototype = {
  handleAppEvent() {
    this.timeSeriesCompareController.destroy();
    this.analysisView.destroyAnalysisOptions();
    // HASH_SERVICE.refreshWindowHashForRouting('analysis');
    const hashParams = HASH_SERVICE.getParams();
    this.analysisModel.init(hashParams);
    this.analysisModel.update(hashParams);
    this.analysisView.init(hashParams);
    this.analysisView.render(hashParams.metricId, () => {this.initTimeSeriesController(hashParams)});
  },

  handleApplyAnalysisEvent(viewObject) {
    const params = viewObject.viewParams;
    this.timeSeriesCompareController.destroy();
    // HASH_SERVICE.clear();
    // params.tab = 'analysis';
    HASH_SERVICE.update(params);
    HASH_SERVICE.refreshWindowHashForRouting('analysis');
    this.initTimeSeriesController(HASH_SERVICE.getParams());
  },

  handleSearchEvent(params = {}) {
    const { searchParams } = params;
    this.timeSeriesCompareController.destroy();
    this.analysisView.destroyAnalysisOptions();

    this.analysisModel.fetchAnalysisOptionsData(searchParams.metricId, 'analysis-spin-area').then((res) => {
      HASH_SERVICE.clear();
      searchParams.tab = 'analysis';
      HASH_SERVICE.update(searchParams);
      HASH_SERVICE.refreshWindowHashForRouting('analysis');
      const hashParams = HASH_SERVICE.getParams();
      this.analysisModel.init(hashParams);
      this.analysisModel.update(hashParams);
      this.analysisView.renderAnalysisOptions();
      this.initTimeSeriesController(hashParams);
    });
  },

  initTimeSeriesController(params) {
    if (params.metricId) {
      this.timeSeriesCompareController.handleAppEvent(params);
    }

  }
};


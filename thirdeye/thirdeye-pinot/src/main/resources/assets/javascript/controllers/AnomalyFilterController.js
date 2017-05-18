function AnomalyFilterController(parentController) {
  this.parentController = parentController;
  this.anomalyFilterModel = new AnomalyFilterModel();
  this.anomalyFilterView = new AnomalyFilterView(this.anomalyFilterModel);

  this.anomalyFilterView.checkedFilterEvent.attach(this.checkedFilterEventHandler.bind(this));
  this.anomalyFilterView.expandedFilterEvent.attach(this.expandedFilterEventHandler.bind(this));
  this.anomalyFilterView.clearEvent.attach(this.clearEventHandler.bind(this));
}

AnomalyFilterController.prototype = {
  handleAppEvent(params) {
    console.log("creating anomaly filters =====");
    this.anomalyFilterModel.init(params);
    this.anomalyFilterView.render();
  },

  checkedFilterEventHandler(sender, args = {}) {
    const {
      filter,
      isChecked,
      section
    } = args;

    const action = isChecked ? 'addFilter' : 'removeFilter';
    this.anomalyFilterModel[action](filter, section);
    // call rerender;
    this.anomalyFilterView.render();
    // this.anomalyFilterView.checkSelectedFilters();
  },

  expandedFilterEventHandler(sender, args = {}) {
    const { filter } = args;
    this.anomalyFilterModel.updatefilterSection(filter);
  },

  resetModel() {
    this.anomalyFilterModel.reset();
  },

  clearEventHandler(sender) {
    this.anomalyFilterModel.clear();
    this.anomalyFilterView.render();
  },

  getSelectedFilters() {
    return [...this.anomalyFilterModel.selectedAnomalies.keys()].join(', ');
  },

  getSelectedAnomalies(pageNumber = 1) {
    const pageSize = constants.ANOMALIES_PER_PAGE;
    const startIndex = (pageNumber - 1) * pageSize;
    const endIndex = startIndex + pageSize;

    return this.anomalyFilterModel.getSelectedAnomalyIds().slice(startIndex, endIndex);
  },

  getTotalAnomalies() {
    return this.anomalyFilterModel.getSelectedAnomalyIds().length;
  },

  getViewFiltersHash() {
    return this.anomalyFilterModel.getViewFiltersHash();
  }
};


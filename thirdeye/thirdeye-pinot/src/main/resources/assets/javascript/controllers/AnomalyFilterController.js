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
      isChecked
    } = args;

    const action = isChecked ? 'addFilter' : 'removeFilter';
    this.anomalyFilterModel[action](filter);
    // call rerender;
    this.anomalyFilterView.render();
  },

  expandedFilterEventHandler(sender, args = {}) {
    const { id } = args;
    this.anomalyFilterModel.updatefilterSection(id);
  },

  clearEventHandler(sender) {
    this.anomalyFilterModel.clear();
    this.anomalyFilterView.render();
  }
};


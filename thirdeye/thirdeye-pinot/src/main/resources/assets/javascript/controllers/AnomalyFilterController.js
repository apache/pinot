/**
 * Logic Control for the search filters other than date range
 */
function AnomalyFilterController() {
  this.anomalyFilterModel = new AnomalyFilterModel();
  this.anomalyFilterView = new AnomalyFilterView(this.anomalyFilterModel);

  this.anomalyFilterView.checkedFilterEvent.attach(this.checkedFilterEventHandler.bind(this));
  this.anomalyFilterView.expandedFilterEvent.attach(this.expandedFilterEventHandler.bind(this));
  this.anomalyFilterView.clearEvent.attach(this.clearEventHandler.bind(this));
}

AnomalyFilterController.prototype = {
  /**
   * Called when searchs are performed
   * @param  {Object} params new search Params
   */
  handleAppEvent(params) {
    this.anomalyFilterModel.init(params);
    this.anomalyFilterView.render();
  },

  /**
   * Event handler for checked filter
   * @param  {Object} sender View the event is originating from
   * @param  {Object} args   Object containing properties of the filter
   */
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
  },

  /**
   * Event Handler for filter section clicks
   * @param  {Object} sender View the event is originating from
   * @param  {Object} args   Object containing the filter name
   */
  expandedFilterEventHandler(sender, args = {}) {
    const { filter } = args;
    this.anomalyFilterModel.updatefilterSection(filter);
  },

  /**
   * Resets the model
   */
  resetModel() {
    this.anomalyFilterModel.reset();
  },

  /**
   * Event Handler for  clear button clicks
   * @param  {Object} sender View the event is originating from
   */
  clearEventHandler(sender) {
    this.anomalyFilterModel.clear();
    this.anomalyFilterView.render();
  },

  /**
   * Get currently selected filters
   * @return {String} Comma delimited strings of anomaly filter names
   */
  getSelectedFilters() {
    return [...this.anomalyFilterModel.selectedAnomalies.keys()].join(', ');
  },

  /**
   * Get anomaly Ids based on filters and page number
   * @param  {Number} pageNumber Page number the user is on
   * @return {Array}             Array of anomaly Ids
   */
  getSelectedAnomalies(pageNumber = 1) {
    const pageSize = constants.ANOMALIES_PER_PAGE;
    const startIndex = (pageNumber - 1) * pageSize;
    const endIndex = startIndex + pageSize;

    return this.anomalyFilterModel.getSelectedAnomalyIds().slice(startIndex, endIndex);
  },

  /**
   * Get Number of Anomalies
   * @return {Number} Total Number of anomalies
   */
  getTotalAnomalies() {
    return this.anomalyFilterModel.getSelectedAnomalyIds().length;
  },

  /**
   * Get Selected Filters in Hash Format
   * @return {Object} Selected filters
   */
  getViewFiltersHash() {
    return this.anomalyFilterModel.getViewFiltersHash();
  }
};


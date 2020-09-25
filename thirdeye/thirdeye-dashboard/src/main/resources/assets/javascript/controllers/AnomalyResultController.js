function AnomalyResultController(parentController) {
  this.parentController = parentController;
  this.anomalyResultModel = new AnomalyResultModel();
  this.anomalyResultView = new AnomalyResultView(this.anomalyResultModel);
  this.anomalyFilterController = new AnomalyFilterController(this);

  this.anomalyResultView.searchButtonEvent.attach(this.searchButtonEventHandler.bind(this));
  this.anomalyResultView.applyButtonEvent.attach(this.applyButtonEventHandler.bind(this));
  this.anomalyResultView.investigateButtonClickEvent.attach(this.investigateButtonClickEventHandler.bind(this));
  this.anomalyResultView.pageClickEvent.attach(this.pageClickEventHandler.bind(this));
  this.anomalyResultView.changedTimeEvent.attach(this.changedTimeEventHandler.bind(this));

  this.anomalyResultView.init();
}

// To do : use total Anomaly from filter

AnomalyResultController.prototype = {
  handleAppEvent() {
    const params = HASH_SERVICE.getParams();
    const hasSameParams = this.anomalyResultModel.hasSameParams(params);
    const hasSearchMode = !!params.anomaliesSearchMode;
    const hasSearchFilters = !!params.searchFilters && Object.keys(params.searchFilters).length;
    const pageNumber = params.pageNumber || 1;

    // returns early if no earch mode
    if (!hasSearchMode) { return; }

    if (hasSameParams) {
      this.updateFilters(hasSearchFilters, pageNumber);
    } else {
      this.clearViewAndUpdateFilters(params, hasSearchFilters);
    }
  },

  /**
   * Update filters depending on search Params
   * @param  {Boolean} hasSearchFilters Does search filters exist
   * @param  {Number}  pageNumber       Current page Number
   */
  updateFilters(hasSearchFilters, pageNumber=1) {
    const anomalyIds = this.anomalyFilterController.getSelectedAnomalies(pageNumber).join(',');
    const totalAnomalies = this.anomalyFilterController.getTotalAnomalies();
    const appliedFilters = this.anomalyFilterController.getSelectedFilters();
    this.anomalyResultModel.setParams({
      pageNumber,
      totalAnomalies,
      appliedFilters
     });
    if (hasSearchFilters) {
      this.anomalyResultModel.getDetailsForAnomalyIds(anomalyIds);
    } else {
      this.anomalyResultModel.setParams({ pageNumber });
      this.anomalyResultModel.rebuild();
    }
  },

  /**
   * Clear view and Update filters depending on search Params
   * @param  {Object} params            Params of the new search
   * @param  {Boolean} hasSearchFilters Does search filters exist
   */
  clearViewAndUpdateFilters(params, hasSearchFilters){
    this.anomalyResultView.destroy();
    this.anomalyResultModel.reset();
    this.anomalyResultModel.setParams(params);
    this.anomalyResultView.renderDatePickers();
    this.anomalyResultModel.getSearchFilters((filter) => {
      filter.selectedFilters = params.searchFilters;
      this.anomalyFilterController.resetModel();
      this.anomalyFilterController.handleAppEvent(filter);
      this.updateFilters(hasSearchFilters, params.pageNumber);
    });
  },

  /**
   * Event Handler for search button clicks
   * @param  {Object} sender View the event is originating from
   * @param  {Object} args   Properties of the new search
   */
  searchButtonEventHandler(sender, args = {}) {
    this.anomalyResultModel.reset();
    HASH_SERVICE.clear();
    HASH_SERVICE.set('tab', 'anomalies');
    HASH_SERVICE.update(args);
    HASH_SERVICE.refreshWindowHashForRouting('anomalies');
    HASH_SERVICE.routeTo('anomalies');
  },

  /**
   * Event Handler for apply filter button clicks
   */
  applyButtonEventHandler() {
    const anomalyIds = this.anomalyFilterController.getSelectedAnomalies().join(',');
    const searchFilters = this.anomalyFilterController.getViewFiltersHash();
    const pageNumber = 1;

    HASH_SERVICE.update({
      searchFilters,
      pageNumber
    });
    HASH_SERVICE.refreshWindowHashForRouting('anomalies');
  },

  /**
   * Event Handler for investigate button clicks
   * @param  {Object} sender View the event is originating from
   * @param  {Object} args   params of the anomaly
   */
  investigateButtonClickEventHandler(sender, args = {}) {
    HASH_SERVICE.clear();
    HASH_SERVICE.set('tab', 'investigate');
    HASH_SERVICE.update(args);
    HASH_SERVICE.refreshWindowHashForRouting('investigate');
    HASH_SERVICE.routeTo('app');
  },

  /**
   * Handles the pagination click by updating the hash params
   * @param  {string} sender Name of the view sending the event
   * @param  {number} pageNumber The page number the user clicked
   */
  pageClickEventHandler(sender, pageNumber) {
    HASH_SERVICE.update({ pageNumber });
    HASH_SERVICE.refreshWindowHashForRouting('anomalies');
    HASH_SERVICE.routeTo('anomalies');
  },

  /**
   * Event Handler for date range picker changes
   * Refetch and rerenders filter on time range changes
   * @param  {Object} sender View the event is originating from
   * @param  {Object} args   containing the new date range
   */
  changedTimeEventHandler(sender, args ={}) {
    // reload filters
    this.anomalyResultModel.reset();
    this.anomalyFilterController.anomalyFilterModel.resetSelection();
    this.anomalyFilterController.anomalyFilterModel.clear();
    HASH_SERVICE.set('tab', 'anomalies');
    HASH_SERVICE.update(args);
    HASH_SERVICE.refreshWindowHashForRouting('anomalies');
    HASH_SERVICE.routeTo('anomalies');
  },
};

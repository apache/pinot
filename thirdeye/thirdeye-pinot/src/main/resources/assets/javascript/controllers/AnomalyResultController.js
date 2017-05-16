function AnomalyResultController(parentController) {
  this.parentController = parentController;
  this.anomalyResultModel = new AnomalyResultModel();
  this.anomalyResultView = new AnomalyResultView(this.anomalyResultModel);
  this.anomalyFilterController = new AnomalyFilterController(this);

  this.anomalyResultView.searchButtonEvent.attach(this.searchButtonEventHandler.bind(this));
  this.anomalyResultView.applyButtonEvent.attach(this.applyButtonEventHandler.bind(this));
  this.anomalyResultView.investigateButtonClickEvent.attach(this.investigateButtonClickEventHandler.bind(this));
  this.anomalyResultView.pageClickEvent.attach(this.pageClickEventHandler.bind(this));
  this.anomalyResultView.filterButtonEvent.attach(this.filterEventHandler.bind(this));
  this.anomalyResultView.checkedFilterEvent.attach(this.checkedFilterEventHandler.bind(this));
  this.anomalyResultView.changedTimeEvent.attach(this.changedTimeEventHandler.bind(this));

  this.anomalyResultView.init();
}

// To do : use total Anomaly from filter

AnomalyResultController.prototype = {
  handleAppEvent: function () {
    const params = HASH_SERVICE.getParams();
    const hasSameParams = this.anomalyResultModel.hasSameParams(params);
    const hasSearchMode = !!params.anomaliesSearchMode;
    const hasSearchFilters = !!params.searchFilters && Object.keys(params.searchFilters).length;
    const pageNumber = params.pageNumber || 1;
    // const hasSearchFilters =
    // if same search dont' rebuild filter
    // has searchFilters and same
    // has search filters not same
    // has none
    if (!hasSearchMode) { return; }
    if (hasSameParams) {
      if (hasSearchFilters) {
        const anomalyIds = this.anomalyFilterController.getSelectedAnomalies(pageNumber).join(',');
        this.anomalyResultModel.setParams({ pageNumber });
        this.anomalyResultModel.getDetailsForAnomalyIds(anomalyIds);
      } else {
        this.anomalyResultModel.setParams({ pageNumber });
        this.anomalyResultModel.rebuild();
      }
    } else {
      // if (hasSearchFilters) {
        // load search filters first
        // if (hasSameSearchFilters) {

        // }
        this.anomalyResultView.renderDatePickers();
        this.anomalyResultView.destroy();
        this.anomalyResultModel.reset();
        this.anomalyResultModel.setParams(params);
        this.anomalyResultModel.getSearchFilters((filter) => {
          filter.selectedFilters = params.searchFilters;
          this.anomalyFilterController.handleAppEvent(filter);
          this.anomalyResultModel.setParams({ totalAnomalies: filter.totalAnomalies });
          if (hasSearchFilters) {
            const anomalyIds = this.anomalyFilterController.getSelectedAnomalies(pageNumber).join(',');
            this.anomalyResultModel.getDetailsForAnomalyIds(anomalyIds);
          } else {
            this.anomalyResultModel.rebuild();
          }
        });
      }
    //   else {
    //     this.anomalyResultView.renderDatePickers();
    //     this.anomalyResultView.destroy();
    //     this.anomalyResultModel.reset();
    //     this.anomalyResultModel.setParams(params);
    //     // need to add conditional redendering for date pickers
    //     // if has searchMode
    //     this.anomalyResultModel.getSearchFilters((filter) => {
    //       filter.selectedFilters = params.searchFilters;
    //       debugger;
    //       this.anomalyResultModel.setParams({ totalAnomalies: filter.totalAnomalies });
    //       this.anomalyFilterController.handleAppEvent(filter);
    //     });
    //     this.anomalyResultModel.rebuild();
    //   }
    // }

    // if () {

    // }
  },

  // new search, should trigger new filters
  searchButtonEventHandler(sender, args = {}) {
    HASH_SERVICE.clear();
    HASH_SERVICE.set('tab', 'anomalies');
    HASH_SERVICE.update(args);
    HASH_SERVICE.refreshWindowHashForRouting('anomalies');
    HASH_SERVICE.routeTo('anomalies');
  },

  applyButtonEventHandler(sender, args = {}) {
    // get anomalies
    const anomalyIds = this.anomalyFilterController.getSelectedAnomalies().join(',');
    // get details for anomalies
    const searchFilters = this.anomalyFilterController.getViewFiltersHash();
    const pageNumber = 1;
    HASH_SERVICE.update({
      searchFilters,
      pageNumber
    });
    HASH_SERVICE.refreshWindowHashForRouting('anomalies');
    // update hash

    // get page
    // display anomalyresult

    // this.anomalyResultModel.getDetailsForAnomalyIds(anomalyIds);
  },


  investigateButtonClickEventHandler(sender, args = {}) {
    HASH_SERVICE.clear();
    HASH_SERVICE.set('tab', 'investigate');
    HASH_SERVICE.update(args);
    HASH_SERVICE.refreshWindowHashForRouting('investigate');
    HASH_SERVICE.routeTo('app');
    // Send this event and the args to parent controller, to route to AnalysisController
  },

  /**
   * Handles the pagination click by updating the hash params
   * @param  {string} sender Name of the view sending the event
   * @param  {number} pageNumber The page number the user clicked
   */
  pageClickEventHandler(sender, pageNumber) {
    HASH_SERVICE.update({ pageNumber });
    HASH_SERVICE.refreshWindowHashForRouting('anomalies');
    // HASH_SERVICE.routeTo('anomalies');
  },

  initFilters() {

  },


  checkedFilterEventHandler(sender, args = {}) {
    const {
      filter,
      isChecked
    } = args;
    const allAnomalies = this.anomalyResultModel.anomaliesWrapper.anomalyids;
    const filters = Object.assign({}, this.anomalyResultModel.anomaliesWrapper.searchFilters);
    let selectedAnomalies = [];
    let viewFilters = {};

    // get selected anomalies
    Object.keys(filters).forEach((key) => {
      const anomalyLists = filters[key];

      if (anomalyLists[filter] && Array.isArray(anomalyLists[filter])) {
        anomalyLists[filter].checked = isChecked;
        selectedAnomalies = anomalyLists[filter];
      }
    });
    debugger;

    // update filter
    Object.keys(filters).forEach((key) => {
      const anomalyLists = filters[key];
      let intersection = [];
      if (Array.isArray(anomalyLists[filter])) {
        intersection = anomalyLists[filter].filter((anomaly) => {
          return selectedAnomalies.includes(anomaly);
        });
        if (intersection.length) {
          viewFilters[filter] = intersection;
        }
      }
    });

    // rerender filters
    debugger;


    //
  },

  changedTimeEventHandler(sender, args ={}) {
    // reload filters
    HASH_SERVICE.update(args);
    HASH_SERVICE.refreshWindowHashForRouting('anomalies');
    HASH_SERVICE.routeTo('anomalies');
  },

  filterEventHandler(sender, args = {}) {
    HASH_SERVICE.update(args);
    HASH_SERVICE.refreshWindowHashForRouting('anomalies');
    HASH_SERVICE.routeTo('anomalies');
  },
};

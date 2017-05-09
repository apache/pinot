function AnomalyResultController(parentController) {
  this.parentController = parentController;
  this.anomalyResultModel = new AnomalyResultModel();
  this.anomalyResultView = new AnomalyResultView(this.anomalyResultModel);
  this.anomalyFiltercontroller = new AnomalyFilterController(this);
  // this.anomalyFilterModel = new AnomalyFilterModel();
  // this.anomalyFilterView = new AnomalyFilterView(this.anomalyFilterModel);

  this.anomalyResultView.applyButtonEvent.attach(this.applyButtonEventHandler.bind(this));
  this.anomalyResultView.investigateButtonClickEvent.attach(this.investigateButtonClickEventHandler.bind(this));
  this.anomalyResultView.pageClickEvent.attach(this.pageClickEventHandler.bind(this));
  this.anomalyResultView.filterButtonEvent.attach(this.filterEventHandler.bind(this));
  this.anomalyResultView.checkedFilterEvent.attach(this.checkedFilterEvent.bind(this));

  this.anomalyResultView.init();
}

AnomalyResultController.prototype = {
  handleAppEvent: function () {
    const params = HASH_SERVICE.getParams();
    const hasSameParams = this.anomalyResultModel.hasSameParams(params);

    // if same search dont' rebuild filter
    if (hasSameParams) {
      this.anomalyResultView.render();
    // } else if (isNewSearch) {
    //   this.
    } else {
      this.anomalyResultView.destroy();
      this.anomalyResultModel.reset();
      this.anomalyResultModel.setParams(params);
      this.anomalyResultModel.getSearchFilters().then((filter) => {
        this.anomalyFiltercontroller.handleAppEvent(filter);
      });
      // this.anomalyResultModel.rebuild();
    }
  },

  // new search, should trigger new filters
  applyButtonEventHandler: function(sender, args) {
    HASH_SERVICE.clear();
    HASH_SERVICE.set('tab', 'anomalies');
    HASH_SERVICE.update(args);
    HASH_SERVICE.refreshWindowHashForRouting('anomalies');
    HASH_SERVICE.routeTo('anomalies');
  },
  investigateButtonClickEventHandler: function (sender, args) {
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
    HASH_SERVICE.routeTo('anomalies');
  },

  initFilters() {

  },


  checkedFilterEvent(sender, args = {}) {
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



  filterEventHandler(sender, args = {}) {
    HASH_SERVICE.update(args);
    HASH_SERVICE.refreshWindowHashForRouting('anomalies');
    HASH_SERVICE.routeTo('anomalies');
  },
};

function AnomalyResultModel() {

  this.anomaliesSearchMode = null;
  this.previousMetricIds = null;
  this.metricIds = null;
  this.dashboardId = null;
  this.previousDashboardId = null;
  this.anomalyIds = null;
  this.anomalyGroupIds = null;
  this.previousAnomalyIds = null

  this.previousStartDate = null;
  this.startDate = moment().subtract(1, 'days').startOf('day');
  this.previousEndDate = null;
  this.endDate = moment().subtract(0, 'days').startOf('day');
  this.functionName = null;
  this.functions = [];
  this.anomalyStatusResolved = true;
  this.anomalyStatusUnresolved = true;

  this.anomaliesWrapper = null;

  this.anomalyForFeedbackUpdate = null;

  this.previousPageNumber = null;
  this.pageNumber = 1;
  this.pageSize = constants.ANOMALIES_PER_PAGE;
  this.totalAnomalies = 0;
  this.ajaxCall = null;
  this.searchFilters = null;
  this.appliedFilters = null;
  this.hiddenFilters = ['statusFilterMap'];
  this.spinner = 'anomaly-spin-area';

  this.renderViewEvent = new Event();
  this.updateModelAndNotifyView = this.updateModelAndNotifyView.bind(this);
}

AnomalyResultModel.prototype = {

  /**
   * Checks whether params are the same
   * @param  {Object}  params New params
   * @return {Boolean}
   */
  hasSameParams(params) {
    if (!params.anomaliesSearchMode || !this.anomaliesSearchMode) {
      return false;
    }

    // Check that the params are tab or rand
    // and is the same as the HASH_PARAMS
    return Object.keys(params)
      .filter(key => params[key])
      .every((key) => {
        const isWhiteListedParam = ['tab', 'rand', 'searchFilters', 'pageNumber'].includes(key);
        const isParamSameAsHash = HASH_PARAMS.isSame(key, params[key], this[key]);

        return isParamSameAsHash || isWhiteListedParam;
      });
  },

  /**
   * Resets the model's search params
   */
  reset() {
    this.metricIds = null;
    this.dashboardId = null;
    this.anomalyIds = null;
    this.anomalyGroupIds = null;
    this.functionName = null;
    this.pageNumber = 1;
    this.totalAnomalies = 0;
    this.startDate = moment().subtract(1, 'days').startOf('day');
    this.endDate = moment().subtract(0, 'days').startOf('day');
  },
  // Call setParams every time there is a change to the model
  setParams : function(params) {
    if (params != undefined) {
      if (params[HASH_PARAMS.ANOMALIES_ANOMALIES_SEARCH_MODE] != undefined) {
        this.anomaliesSearchMode = params[HASH_PARAMS.ANOMALIES_ANOMALIES_SEARCH_MODE];
      }
      if (params[HASH_PARAMS.ANOMALIES_METRIC_IDS] != undefined) {
        this.metricIds = params[HASH_PARAMS.ANOMALIES_METRIC_IDS];
      }
      if (params[HASH_PARAMS.ANOMALIES_DASHBOARD_ID] != undefined) {
        this.dashboardId = params[HASH_PARAMS.ANOMALIES_DASHBOARD_ID];
      }
      if (params[HASH_PARAMS.ANOMALIES_ANOMALY_IDS] != undefined) {
        this.anomalyIds = params[HASH_PARAMS.ANOMALIES_ANOMALY_IDS];
      }

      this.anomalyGroupIds = params[HASH_PARAMS.ANOMALIES_GROUP_IDS] || this.anomalyGroupIds;

      if (params[HASH_PARAMS.ANOMALIES_START_DATE] != undefined) {
        this.startDate = params[HASH_PARAMS.ANOMALIES_START_DATE];
      }
      if (params[HASH_PARAMS.ANOMALIES_END_DATE] != undefined) {
        this.endDate = params[HASH_PARAMS.ANOMALIES_END_DATE];
      }
      if (params[HASH_PARAMS.ANOMALIES_PAGE_NUMBER] != undefined) {
        this.pageNumber = params[HASH_PARAMS.ANOMALIES_PAGE_NUMBER];
      }
      if (params['functionName'] != undefined) {
        this.functionName = params['functionName'];
      }
      if (params['feedback'] != undefined) {
        var idx = params['idx'];
        this.anomaliesWrapper.anomalyDetailsList[idx].anomalyFeedback = params['feedback'];
        this.anomalyForFeedbackUpdate = this.anomaliesWrapper.anomalyDetailsList[idx];
      }
      this.searchFilters = params.searchFilters ? Object.assign({}, params.searchFilters) : this.searchFilters;
      this.totalAnomalies = params.totalAnomalies || this.totalAnomalies;
      this.appliedFilters = params.appliedFilters;
    }
  },

  /**
   * fetches anomalies based on new search params
   * Aborts previous search request if it exists
   */
  rebuild() {
    if (this.ajaxCall && !this.ajaxCall.status) {
      this.ajaxCall.abort();
    }
    const params = this.getSearchParams();
    this.ajaxCall = dataService.fetchAnomalies(params);
  },


  /**
   * Helper function that gets all relevant properties for search
   * @return {Obj} Object containing all needed params
   */
  getSearchParams() {
    return function(obj) {
      const {
          anomaliesSearchMode,
          startDate,
          endDate,
          pageNumber,
          metricIds,
          dashboardId,
          anomalyIds,
          anomalyGroupIds,
          functionName,
          updateModelAndNotifyView,
          spinner
      } = obj;

      return {
        anomaliesSearchMode,
        startDate,
        endDate,
        pageNumber,
        metricIds,
        dashboardId,
        anomalyIds,
        anomalyGroupIds,
        functionName,
        updateModelAndNotifyView,
        spinner
      };
    }(this);
  },

  /**
   * Gets Search filters and cancel previous call
   * @param  {Function} callback Function to be called after filters are retrieved
   */
  getSearchFilters(callback) {
    if (this.searchAjaxCall && !this.searchAjaxCall.status) {
      this.searchAjaxCall.abort();
    }
    const params = this.getSearchParams();
    params.filterOnly = true;
    params.updateModelAndNotifyView = callback;
    params.spinner = 'anomaly-filter-spinner';
    this.searchAjaxCall = dataService.fetchAnomalies(params);
  },

  /**
   * Fetches details for given anomaly Ids
   * @param  {Array}  anomalyIds Array of anomaly Ids to getch
   */
  getDetailsForAnomalyIds(anomalyIds = []) {
    if (!anomalyIds.length) { return; }
    if (this.ajaxCall && !this.ajaxCall.status) {
      this.ajaxCall.abort();
    }
    const params = {};
    params.startDate = this.startDate;
    params.endDate = this.endDate;
    params.pageNumber = 1;
    params.anomaliesSearchMode = constants.MODE_ID;
    params.updateModelAndNotifyView = this.updateModelAndNotifyView;
    params.anomalyIds = anomalyIds;
    params.spinner = this.spinner;
    this.ajaxCall = dataService.fetchAnomalies(params);
  },
  /**
   * Call Back function that rerenders the view
   * @param  {Object} anomaliesWrapper Result payload of search
   */
  updateModelAndNotifyView(anomaliesWrapper) {
    this.anomaliesWrapper = anomaliesWrapper;
    this.formatAnomalies();
    this.renderViewEvent.notify();
  },

  /**
   * Helper Function that returns formatted anomaly region duration data for UI
   * @param  {date}   start       the anomaly region start
   * @param  {date}   end         the anomaly region end
   * @param  {string} granularity the granularity of the anomaly
   * @return {string}         formatted start - end date/time
   */
  getRegionDuration(start, end, granularity) {

    if (!(start && end)) {
      return 'N/A';
    }
    const regionStart = moment.tz(start, constants.TIMESERIES_DATE_FORMAT, constants.TIME_ZONE);
    const regionEnd = moment.tz(end, constants.TIMESERIES_DATE_FORMAT, constants.TIME_ZONE);
    const isSameDay = regionStart.isSame(regionEnd, 'day');
    const timeDelta = regionEnd.diff(regionStart);
    const regionDuration = moment.duration(timeDelta);
    const showTime = granularity !== constants.GRANULARITY_DAY;
    let range = '';
    let regionStartFormat = constants.DETAILS_DATE_FORMAT;
    let regionEndFormat = constants.DETAILS_DATE_FORMAT;

    if (showTime) {
      regionStartFormat += `, ${constants.DETAILS_DATE_HOURS_FORMAT}`;
      regionEndFormat += `, ${constants.DETAILS_DATE_HOURS_FORMAT}`;
    }

    if (isSameDay) {
      regionEndFormat = '';
    }

    if (isSameDay && showTime) {
      regionEndFormat = constants.DETAILS_DATE_HOURS_FORMAT;
    }

    return `${regionDuration.humanize()} (${regionStart.format(regionStartFormat)}${regionEndFormat ? ' - ' + regionEnd.format(regionEndFormat) : ''})`;
  },

  /**
   * Helper Function that retuns formatted change delta for UI
   * @param  {int}    current    current average anomaly
   * @param  {int}    baseline   baseline the anomaly is compared too
   * @return {string}            'N/A' if either is missing, otherwise formatted delta (%)
   */
  getChangeDelta(current, baseline) {
    let changeDelta = 'N/A';
    if (current && baseline) {
      const amount = (current - baseline) / baseline * 100;
      changeDelta = `${amount.toFixed(2)}%`;
    }

    return changeDelta;
  },

  /**
   * Helper Function that sets formatted duration and changeDelta onto the anomaly model
   * @return {null}
   */
  formatAnomalies() {
    this.anomaliesWrapper.anomalyDetailsList.forEach((anomaly) => {
      anomaly.duration = this.getRegionDuration(anomaly.anomalyStart, anomaly.anomalyEnd, anomaly.timeUnit);
      anomaly.changeDelta = this.getChangeDelta(anomaly.current, anomaly.baseline);
    });
  },

  // Instead of calling rebuild for a simple anomaly feedback change, made a smaller function
  updateAnomalyFeedback : function() {
    var feedbackType = this.getFeedbackTypeFromString(this.anomalyForFeedbackUpdate.anomalyFeedback);
    dataService.updateFeedback(this.anomalyForFeedbackUpdate.anomalyId, feedbackType);
  },
  getAnomaliesWrapper() {
    return Object.assign(this.anomaliesWrapper, {
      totalAnomalies: this.totalAnomalies,
      appliedFilters: this.appliedFilters
    });
  },

  /**
   * Return non empty filters
   * @return {Object} Subset of searchFilters
   */
  getAnomaliesFilters() {
    const anomaliesFilters = this.anomaliesWrapper.searchFilters || {};

    return Object.keys(anomaliesFilters)
      .filter(key => Object.keys(anomaliesFilters[key]).length && !this.hiddenFilters.includes(key))
      .reduce((filters, key) => {
        filters[key] = anomaliesFilters[key];
        return filters;
      }, {});
  },

  /**
   * Function Getter
   * @return {Array} Array of functions
   */
  getAnomalyFunctions() {
    return this.functions;
  },
  // Helper functions to convert between UI string for feedback to database enum
  getFeedbackTypeFromString : function(feedback) {
    switch (feedback) {
    case constants.FEEDBACK_STRING_CONFIRMED_ANOMALY:
      return constants.FEEDBACK_TYPE_ANOMALY;
    case constants.FEEDBACK_STRING_FALSE_ALARM:
      return constants.FEEDBACK_TYPE_NOT_ANOMALY;
    case constants.FEEDBACK_STRING_CONFIRMED_NEW_TREND:
      return constants.FEEDBACK_TYPE_ANOMALY_NEW_TREND;
    default:
      return feedbackTypeStr;
    }
  },
  getFeedbackStringFromType : function(feedbackType) {
    switch (feedbackType) {
    case constants.FEEDBACK_TYPE_ANOMALY:
      return constants.FEEDBACK_STRING_CONFIRMED_ANOMALY;
    case constants.FEEDBACK_TYPE_NOT_ANOMALY:
      return constants.FEEDBACK_STRING_FALSE_ALARM;
    case constants.FEEDBACK_TYPE_ANOMALY_NEW_TREND:
      return constants.FEEDBACK_STRING_CONFIRMED_NEW_TREND;
    default:
      return feedbackType;
    }
  },
};

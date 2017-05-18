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
  this.pageSize = 10;

  this.renderViewEvent = new Event();

}

AnomalyResultModel.prototype = {
  hasSameParams(params) {
    if (!params.anomaliesSearchMode || !this.anomaliesSearchMode) {
      return false;
    }

    // Check that the params are tab or rand
    // and is the same as the HASH_PARAMS
    return Object.keys(params).every((key) => {
      const isParamTabOrRandom = ['tab', 'rand'].includes(key);
      const isParamSameAsHash = !!this[key] && HASH_PARAMS.isSame(key, params[key], this[key]);

      return isParamTabOrRandom || isParamSameAsHash;
    });
  },

  reset : function() {
    this.metricIds = null;
    this.dashboardId = null;
    this.anomalyIds = null;
    this.anomalyGroupIds = null;
    this.functionName = null;
    this.pageNumber = 1;

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
    }
  },
  // Call rebuild every time new anomalies are to be loaded with new model
  rebuild : function() {
    if (this.anomaliesSearchMode == constants.MODE_METRIC && this.metricIds != undefined && this.metricIds.length > 0) {
      dataService.fetchAnomaliesForMetricIds(
          this.startDate, this.endDate, this.pageNumber, this.metricIds, this.functionName, this.updateModelAndNotifyView.bind(this));
    } else if (this.anomaliesSearchMode == constants.MODE_DASHBOARD && this.dashboardId != undefined) {
      dataService.fetchAnomaliesForDashboardId(
          this.startDate, this.endDate, this.pageNumber, this.dashboardId, this.functionName, this.updateModelAndNotifyView.bind(this));
    } else if (this.anomaliesSearchMode == constants.MODE_ID && this.anomalyIds != undefined && this.anomalyIds.length > 0 && this.anomalyIds != this.previousAnomalyIds) {
      dataService.fetchAnomaliesForAnomalyIds(
          this.startDate, this.endDate, this.pageNumber, this.anomalyIds, this.functionName, this.updateModelAndNotifyView.bind(this));
    } else if (this.anomaliesSearchMode == constants.MODE_TIME) {
      dataService.fetchAnomaliesForTime(this.startDate, this.endDate, this.pageNumber, this.updateModelAndNotifyView.bind(this));
    } else if (this.anomaliesSearchMode == constants.MODE_GROUPID && this.anomalyGroupIds != undefined && this.anomalyGroupIds.length > 0) {
      dataService.fetchAnomaliesforGroupIds(this.startDate, this.endDate, this.pageNumber, this.anomalyGroupIds, this.functionName, this.updateModelAndNotifyView.bind(this));
    }
  },
  updateModelAndNotifyView : function(anomaliesWrapper) {
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
  getAnomaliesWrapper : function() {
    return this.anomaliesWrapper;
  },
  getAnomalyFunctions : function() {
    return this.functions;
  },
  // Helper functions to convert between UI string for feedback to database enum
  getFeedbackTypeFromString : function(feedback) {
    switch (feedback) {
    case constants.FEEDBACK_STRING_CONFIRMED_ANOMALY:
      return constants.FEEDBACK_TYPE_ANOMALY;
    case constants.FEEDBACK_STRING_FALSE_ALARM:
      return constants.FEEDBACK_TYPE_NOT_ANOMALY;
    case constants.FEEDBACK_STRING_CONFIRMED_NOT_ACTIONABLE:
      return constants.FEEDBACK_TYPE_ANOMALY_NO_ACTION;
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
    case constants.FEEDBACK_TYPE_ANOMALY_NO_ACTION:
      return constants.FEEDBACK_STRING_CONFIRMED_NOT_ACTIONABLE;
    default:
      return feedbackType;
    }
  },
}

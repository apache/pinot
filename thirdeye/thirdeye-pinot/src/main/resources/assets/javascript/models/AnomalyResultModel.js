function AnomalyResultModel() {

  this.anomaliesSearchMode = "time";
  this.previousMetricIds = null;
  this.metricIds = null;
  this.dashboardId = null;
  this.previousDashboardId = null;
  this.anomalyIds = null;
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
  reset : function() {
    console.log('Reset of anomaly result model');
    this.metricIds = null;
    this.dashboardId = null;
    this.anomalyIds = null;
    this.functionName = null;
    this.pageNumber = 1;

  },
  // Call setParams every time there is a change to the model
  setParams : function() {
    console.log("Set params of Anomaly Result Model");
    var params = HASH_SERVICE.getParams();
    if (params != undefined) {
      console.log("params");
      if (params[HASH_PARAMS.ANOMALIES_ANOMALIES_SEARCH_MODE] != undefined) {
        console.log('anomaliesSearchMode');
        this.anomaliesSearchMode = params[HASH_PARAMS.ANOMALIES_ANOMALIES_SEARCH_MODE];
      }
      if (params[HASH_PARAMS.ANOMALIES_METRIC_IDS] != undefined) {
        console.log("metricIds");
        this.metricIds = params[HASH_PARAMS.ANOMALIES_METRIC_IDS];
        console.log(this.metricIds);
      }
      if (params[HASH_PARAMS.ANOMALIES_DASHBOARD_ID] != undefined) {
        console.log("dashboardId");
        this.dashboardId = params[HASH_PARAMS.ANOMALIES_DASHBOARD_ID];
        console.log(this.dashboardId);
      }
      if (params[HASH_PARAMS.ANOMALIES_ANOMALY_IDS] != undefined) {
        console.log("anomalyIds");
        this.anomalyIds = params[HASH_PARAMS.ANOMALIES_ANOMALY_IDS];
        console.log(this.anomalyIds);
      }
      if (params[HASH_PARAMS.ANOMALIES_START_DATE] != undefined) {
        console.log("startDate");
        this.startDate = params[HASH_PARAMS.ANOMALIES_START_DATE];
      }
      if (params[HASH_PARAMS.ANOMALIES_END_DATE] != undefined) {
        console.log("endDate");
        this.endDate = params[HASH_PARAMS.ANOMALIES_END_DATE];
      }
      if (params[HASH_PARAMS.ANOMALIES_PAGE_NUMBER] != undefined) {
        console.log("pageNumber");
        this.pageNumber = params[HASH_PARAMS.ANOMALIES_PAGE_NUMBER];
      }
      if (params['functionName'] != undefined) {
        console.log("functionName");
        this.functionName = params['functionName'];
      }
      if (params['feedback'] != undefined) {
        console.log("feedback");
        var idx = params['idx'];
        this.anomaliesWrapper.anomalyDetailsList[idx].anomalyFeedback = params['feedback'];
        this.anomalyForFeedbackUpdate = this.anomaliesWrapper.anomalyDetailsList[idx];
      }
    }
  },
  // Call rebuild every time new anomalies are to be loaded with new model
  rebuild : function() {
    if (this.anomaliesSearchMode == constants.MODE_METRIC && this.metricIds != undefined && this.metricIds.length > 0 && this.metricIds != this.previousMetricIds) {
      this.previousMetricIds = this.metricIds;
      dataService.fetchAnomaliesForMetricIds(
          this.startDate, this.endDate, this.pageNumber, this.metricIds, this.functionName, this.updateModelAndNotifyView.bind(this));
    } else if (this.anomaliesSearchMode == constants.MODE_DASHBOARD && this.dashboardId != undefined && this.dashboardId != this.previousDashboardId) {
      this.previousDashboardId = this.dashboardId;
      dataService.fetchAnomaliesForDashboardId(
          this.startDate, this.endDate, this.pageNumber, this.dashboardId, this.functionName, this.updateModelAndNotifyView.bind(this));
    } else if (this.anomaliesSearchMode == constants.MODE_ID && this.anomalyIds != undefined && this.anomalyIds.length > 0 && this.anomalyIds != this.previousAnomalyIds) {
      this.previousAnomalyIds = this.anomalyIds;
      dataService.fetchAnomaliesForAnomalyIds(
          this.startDate, this.endDate, this.pageNumber, this.anomalyIds, this.functionName, this.updateModelAndNotifyView.bind(this));
    } else if (this.anomaliesSearchMode == constants.MODE_TIME && (this.pageNumber != this.previousPageNumber || this.previousStartDate != this.startDate || this.previousEndDate != this.endDate)) {
      this.previousStartDate = this.startDate;
      this.previousEndDate = this.endDate;
      this.previousPageNumber = this.pageNumber;
      dataService.fetchAnomaliesForTime(this.startDate, this.endDate, this.pageNumber, this.updateModelAndNotifyView.bind(this));
    }
  },
  updateModelAndNotifyView : function(anomaliesWrapper) {
    this.anomaliesWrapper = anomaliesWrapper;
    this.renderViewEvent.notify();
  },
  // Instead of calling rebuild for a simple anomaly feedback change, made a smaller function
  updateAnomalyFeedback : function() {
    console.log("Updating feedback at backend");
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
  }
}

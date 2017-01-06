function AnomalyResultModel() {

  this.anomaliesSearchMode = "metric";
  this.metricIds = [];
  this.dashboardId = null;
  this.anomalyIds = [];

  this.previousStartDate = null;
  this.startDate = moment().subtract(6, 'days').startOf('day');
  this.previousEndDate = null;
  this.endDate = moment().subtract(0, 'days').startOf('day');
  this.functionName = null;
  this.functions = [];
  this.anomalyStatusResolved = true;
  this.anomalyStatusUnresolved = true;

  this.anomaliesWrapper = null;

  this.anomalyForFeedbackUpdate = null;

  this.renderViewEvent = new Event();

}

AnomalyResultModel.prototype = {
  reset : function() {
    this.metricIds = [];
    this.dashboardId = null;
    this.anomalyIds = [];
    this.functionName = null;

  },
  // Call setParams every time there is a change to the model
  setParams : function(params) {
    console.log(params);
    if (params != undefined) {
      console.log("params");
      if (params['mode'] != undefined) {
        console.log('mode');
        this.anomaliesSearchMode = params['mode'];
      }
      if (params['metricIds'] != undefined) {
        console.log("metricIds");
        this.metricIds = params['metricIds'];
      }
      if (params['dashboardId'] != undefined) {
        console.log("dashboardId");
        this.dashboardId = params['dashboardId'];
      }
      if (params['anomalyIds'] != undefined) {
        console.log("anomalyIds");
        this.anomalyIds = params['anomalyIds'];
      }
      if (params['startDate'] != undefined) {
        console.log("startDate");
        this.startDate = params['startDate'];
      }
      if (params['endDate'] != undefined) {
        console.log("endDate");
        this.endDate = params['endDate'];
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
    if (this.anomaliesSearchMode == constants.MODE_METRIC && this.metricIds != undefined && this.metricIds.length > 0) {
      dataService.fetchAnomaliesForMetricIds(
          this.startDate, this.endDate, this.metricIds, this.functionName, this.updateModelAndNotifyView.bind(this));
    } else if (this.anomaliesSearchMode == constants.MODE_DASHBOARD && this.dashboardId != undefined) {
      dataService.fetchAnomaliesForDashboardId(
          this.startDate, this.endDate, this.dashboardId, this.functionName, this.updateModelAndNotifyView.bind(this));
    } else if (this.anomaliesSearchMode == constants.MODE_ID && this.anomalyIds != undefined && this.anomalyIds.length > 0) {
      dataService.fetchAnomaliesForAnomalyIds(
          this.startDate, this.endDate, this.anomalyIds, this.functionName, this.updateModelAndNotifyView.bind(this));
    } else if (this.anomaliesSearchMode == constants.MODE_TIME && (this.previousStartDate != this.startDate || this.previousEndDate != this.endDate)) {
      this.previousStartDate = this.startDate;
      this.previousEndDate = this.endDate;
      dataService.fetchAnomaliesForTime(
          this.startDate, this.endDate, this.updateModelAndNotifyView.bind(this));
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

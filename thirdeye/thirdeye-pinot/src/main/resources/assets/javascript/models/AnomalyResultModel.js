function AnomalyResultModel() {

  this.anomaliesSearchMode = "metric";
  this.metricIds = [];
  this.dashboardId = null;
  this.anomalyIds = [];

  this.startDate = moment().subtract(6, 'days').startOf('day');
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
    }
  },
  // TODO: change return value of anomalies to complex object, instead of array
  // so that we can pass information such as total number of anomalies (this if for the "Showing x anomalies of y")
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

function AnomalyWrapper() {
  this.anomalyId = "101";
  this.metric = "feed_sessions_additive";
  this.dataset = "engaged_feed_session_count"

  this.dates = [ '2016-01-01', '2016-01-02', '2016-01-03', '2016-01-04', '2016-01-05', '2016-01-06', '2016-01-07' ];
  this.currentEnd = 'Jan 7 2016';
  this.currentStart = 'Jan 1 2016';
  this.baselineEnd = 'Dec 31 2015';
  this.baselineStart = 'Dec 25 2015';
  this.baselineValues = [ 35, 225, 200, 600, 170, 220, 70 ];
  this.currentValues = [ 30, 200, 100, 400, 150, 250, 60 ];
  this.current = '1000';
  this.baseline = '2000';

  this.anomalyRegionStart = '2016-01-03';
  this.anomalyRegionEnd = '2016-01-05';
  this.anomalyFunctionId = 5;
  this.anomalyFunctionName = 'efs_wow_country';
  this.anomalyFunctionType = 'wow_rule';
  this.anomalyFunctionProps = 'props,props,props';
  this.anomalyFunctionDimension = 'country:US';
  this.anomalyFeedback = "Confirmed Anomaly";
}

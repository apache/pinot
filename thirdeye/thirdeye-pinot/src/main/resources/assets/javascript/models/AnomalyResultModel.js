function AnomalyResultModel() {

  this.anomaliesSearchTab = 'anomalies_search-by-metric';
  this.anomaliesTabText = 'Metrics';
  this.metricIds = [];
  this.dashboardId = null;
  this.anomalyIds = [];

  this.startDate = moment().subtract(6, 'days').startOf('day');
  this.endDate = moment().subtract(0, 'days').startOf('day');
  this.functionName = null;
  this.functions = [];
  this.anomalyStatusResolved = true;
  this.anomalyStatusUnresolved = true;

  this.anomalies = [];

}

AnomalyResultModel.prototype = {
  reset : function() {
    this.metricIds = [];
    this.dashboardId = null;
    this.anomalyIds = [];
    this.functionName = null;
  },
  setParams : function(params) {
    this.reset();
    console.log(params);
    if (params != undefined) {
      this.anomaliesTabText = params['anomaliesTabText'];
      switch(this.anomaliesTabText) {
        case constants.ANOMALIES_TAB_TEXT_DASHBOARD:
          this.anomaliesSearchTab = 'anomalies_search-by-dashboard'
            break;
        case constants.ANOMALIES_TAB_TEXT_ID:
          this.anomaliesSearchTab = 'anomalies_search-by-id'
            break;
        case constants.ANOMALIES_TAB_TEXT_METRICS:
        default:
          this.anomaliesSearchTab = 'anomalies_search-by-metric'
      }
      if (params['metricIds'] != undefined) {
        this.metricIds = params['metricIds'];
      }
      if (params['dashboardId'] != undefined) {
        this.dashboardId = params['dashboardId'];
      }
      if (params['anomalyIds'] != undefined) {
        this.anomalyIds = params['anomalyIds'];
      }
      this.startDate = params['startDate'];
      this.endDate = params['endDate'];
      if (params['functionName'] != undefined) {
        this.functionName = params['functionName'];
      }
    }
  },

  rebuild : function() {
    var anomalies = [];
    if (this.anomaliesTabText == constants.ANOMALIES_TAB_TEXT_METRICS && this.metricIds != undefined && this.metricIds.length > 0) {
      anomalies = dataService.fetchAnomaliesForMetricIds(this.startDate, this.endDate, this.metricIds, this.functionName);
    } else if (this.anomaliesTabText == constants.ANOMALIES_TAB_TEXT_DASHBOARD && this.dashboardId != undefined) {
      anomalies = dataService.fetchAnomaliesForDashboardId(this.startDate, this.endDate, this.dashboardId, this.functionName);
    } else if (this.anomaliesTabText == constants.ANOMALIES_TAB_TEXT_ID && this.anomalyIds != undefined && this.anomalyIds.length > 0) {
      anomalies = dataService.fetchAnomaliesForAnomalyIds(this.startDate, this.endDate, this.anomalyIds, this.functionName);
    }
    this.anomalies = anomalies;

  },
  getAnomaliesList : function() {
    return this.anomalies;
  },
  getAnomalyFunctions : function() {
    return this.functions;
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

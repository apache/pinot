function Constants() {

  // constants
  this.METRIC_AUTOCOMPLETE_QUERY_PARAM = 'name';
  this.DASHBOARD_AUTOCOMPLETE_QUERY_PARAM = 'name';
  this.ANOMALY_AUTOCOMPLETE_QUERY_PARAM = 'id';

  // endpoints
  this.METRIC_AUTOCOMPLETE_ENDPOINT = '/data/autocomplete/metric';
  this.DASHBOARD_AUTOCOMPLETE_ENDPOINT = '/data/autocomplete/dashboard';
  this.ANOMALY_AUTOCOMPLETE_ENDPOINT = '/anomalies/autocomplete/anomalyId';
  this.METRIC_SUMMARY = '/data/dashboard/metricsummary';
  this.ANOMALY_SUMMARY = '/data/dashboard/anomalysummary';
  this.WOW_SUMMARY = '/data/dashboard/wowsummary';
  this.SEARCH_ANOMALIES_METRICIDS = '/anomalies/search/metricIds/';
  this.SEARCH_ANOMALIES_DASHBOARDID = '/anomalies/search/dashboardId/';
  this.SEARCH_ANOMALIES_ANOMALYIDS = '/anomalies/search/anomalyIds/';
  this.SEARCH_ANOMALIES_TIME = '/anomalies/search/time/';
  this.SEARCH_ANOMALIES_GROUPIDS = '/anomalies/search/anomalyGroupIds/';
  this.UPDATE_ANOMALY_FEEDBACK = '/anomalies/updateFeedback/';
  this.METRIC_GRANULARITY = '/data/agg/granularity/metric/';
  this.METRIC_DIMENSION = '/data/autocomplete/dimensions/metric/';
  this.METRIC_FILTERS = '/data/autocomplete/filters/metric/';
  this.METRIC_MAX_TIME = '/data/maxDataTime/metricId/';
  this.MAX_POINT_NUM = 150;

  //
  this.TIMESERIES_DATE_FORMAT = 'YYYY-MM-DD HH:mm';
  this.DETAILS_DATE_FORMAT = 'MMM D';
  this.DETAILS_DATE_DAYS_FORMAT = 'MMM D, HH:mm z';
  this.DETAILS_DATE_HOURS_FORMAT = 'HH:mm z';
  this.DATE_RANGE_CUSTOM = 'Custom Range';
  this.DATE_RANGE_FORMAT = 'MMM D, YYYY';
  this.DATE_HOUR_RANGE_FORMAT = 'MMM D, YYYY h a';
  this.DATE_TIME_RANGE_FORMAT = 'MMM D, YYYY hh:mm a';
  this.TIME_ZONE = 'America/Los_Angeles';

  // modes
  this.MODE_METRIC = 'metric';
  this.MODE_DASHBOARD = 'dashboard';
  this.MODE_ID = 'id';
  this.MODE_GROUPID = 'groupId'
  this.MODE_TIME = 'time';

  this.DASHBOARD_MODE_METRIC_SUMMARY = 'MetricSummary';
  this.DASHBOARD_MODE_ANOMALY_SUMMARY = 'AnomalySummary';
  this.DASHBOARD_MODE_WOW_SUMMARY = 'WowSummary';

  // tabs
  this.TAB_DASHBOARD = 'dashboard';
  this.TAB_ANOMALIES = 'anomalies';
  this.TAB_ANALYSIS = 'analysis';
  this.TAB_INVESTIGATE = 'investigate';

  this.DEFAULT_ANALYSIS_GRANULARITY = 'DAYS';
  this.DEFAULT_ANALYSIS_DIMENSION = 'All';

  this.FEEDBACK_STRING_CONFIRMED_ANOMALY = 'Confirmed Anomaly';
  this.FEEDBACK_STRING_FALSE_ALARM = 'False Alarm';
  this.FEEDBACK_STRING_CONFIRMED_NEW_TREND = 'Confirmed - New Trend';
  this.FEEDBACK_TYPE_ANOMALY = 'ANOMALY';
  this.FEEDBACK_TYPE_NOT_ANOMALY = 'NOT_ANOMALY';
  this.FEEDBACK_TYPE_ANOMALY_NEW_TREND = 'ANOMALY_NEW_TREND';

  this.FEEDBACK_TYPE_MAPPING = {
    ANOMALY: 'Confirmed Anomaly',
    ANOMALY_NEW_TREND: 'Confirmed - New Trend',
    NOT_ANOMALY: 'False Alarm'
  };

  this.DEFAULT_COMPARE_MODE = 'WoW';
  this.COMPARE_MODE_OPTIONS = ['WoW', 'Wo2W', 'Wo3W'];

  this.GRANULARITY_DAY = 'DAYS';
  this.ANOMALIES_PER_PAGE = 10;
  this.WOW_MAPPING = {
    WoW: 7,
    Wo2W: 14,
    Wo3W: 21,
    Wo4W: 28
  };
}

Constants.prototype = {


};

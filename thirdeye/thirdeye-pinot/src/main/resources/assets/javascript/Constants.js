function Constants() {

  // constants
  this.METRIC_AUTOCOMPLETE_QUERY_PARAM = 'name';
  this.DASHBOARD_AUTOCOMPLETE_QUERY_PARAM = 'name';
  this.ANOMALY_AUTOCOMPLETE_QUERY_PARAM = 'id';

  // endpoints
  this.METRIC_AUTOCOMPLETE_ENDPOINT = '/data/autocomplete/metric';
  this.DASHBOARD_AUTOCOMPLETE_ENDPOINT = '/data/autocomplete/dashboard';
  this.ANOMALY_AUTOCOMPLETE_ENDPOINT = '/anomalies/autocomplete/anomalyId';
  this.SEARCH_ANOMALIES_METRICIDS = '/anomalies/search/metricIds/';
  this.SEARCH_ANOMALIES_DASHBOARDID = '/anomalies/search/dashboardId/';
  this.SEARCH_ANOMALIES_ANOMALYIDS = '/anomalies/search/anomalyIds/';

  this.TIMESERIES_DATE_FORMAT = 'YYYY-MM-DD HH:mm';
  this.DETAILS_DATE_FORMAT = 'MMM DD YYYY HH:mm';

  this.ANOMALIES_TAB_TEXT_METRICS = 'Metrics';
  this.ANOMALIES_TAB_TEXT_DASHBOARD = 'Dashboard';
  this.ANOMALIES_TAB_TEXT_ID = 'ID';
}

Constants.prototype = {


};

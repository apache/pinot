function HashParams() {

  this.TAB = 'tab';

  this.DASHBOARD_MODE = 'mode';
  this.DASHBOARD_DASHBOARD_NAME = 'dashboardName';
  this.DASHBOARD_SUMMARY_DASHBOARD_ID = 'summaryDashboardId';

  this.ANOMALIES_ANOMALIES_SEARCH_MODE = 'anomaliesSearchMode';
  this.ANOMALIES_START_DATE = 'startDate';
  this.ANOMALIES_END_DATE = 'endDate';
  this.ANOMALIES_PAGE_NUMBER = 'pageNumber';
  this.ANOMALIES_METRIC_IDS = 'metricIds';
  this.ANOMALIES_DASHBOARD_ID = 'dashboardId';
  this.ANOMALIES_ANOMALY_IDS = 'anomalyIds';

  this.RAND = 'rand';

  // this map has key = <controller name> and value = <map of param names and its default value>
  this.controllerNameToParamNamesMap = {};
  this.APP_CONTROLLER = "app";
  this.DASHBOARD_CONTROLLER = "dashboard";
  this.ANOMALIES_CONTROLLER = "anomalies";
  this.ANALYSIS_CONTROLLER = "analysis";
  this.DASHBOARD_ANOMALY_SUMMARY_CONTROLLER = "anomalySummary";
  this.DASHBOARD_METRIC_SUMMARY_CONTROLLER = "metricSummary";
  this.DASHBOARD_WOW_SUMMARY_CONTROLLER = "wowSummary";
}


HashParams.prototype = {
    init : function() {
      // appController
      var paramNamesToDefaultValuesMap = {};
      paramNamesToDefaultValuesMap[this.TAB] = constants.TAB_ANOMALIES;
      this.controllerNameToParamNamesMap[this.APP_CONTROLLER] = paramNamesToDefaultValuesMap;

      // dashboardController
      paramNamesToDefaultValuesMap = {};
      paramNamesToDefaultValuesMap[this.TAB] = constants.TAB_DASHBOARD;
      paramNamesToDefaultValuesMap[this.DASHBOARD_MODE] = constants.DASHBOARD_MODE_ANOMALY_SUMMARY;
      paramNamesToDefaultValuesMap[this.DASHBOARD_DASHBOARD_NAME] = undefined;
      paramNamesToDefaultValuesMap[this.DASHBOARD_SUMMARY_DASHBOARD_ID] = undefined;
      this.controllerNameToParamNamesMap[this.DASHBOARD_CONTROLLER] = paramNamesToDefaultValuesMap;

      // dashboard anomalySummary Controller
      paramNamesToDefaultValuesMap = {};
      paramNamesToDefaultValuesMap[this.TAB] = constants.TAB_DASHBOARD;
      paramNamesToDefaultValuesMap[this.DASHBOARD_MODE] = constants.DASHBOARD_MODE_ANOMALY_SUMMARY;
      paramNamesToDefaultValuesMap[this.DASHBOARD_DASHBOARD_NAME] = undefined;
      paramNamesToDefaultValuesMap[this.DASHBOARD_SUMMARY_DASHBOARD_ID] = undefined;
      this.controllerNameToParamNamesMap[this.DASHBOARD_ANOMALY_SUMMARY_CONTROLLER] = paramNamesToDefaultValuesMap;

      // dashboard metricSummary Controller
      paramNamesToDefaultValuesMap = {};
      paramNamesToDefaultValuesMap[this.TAB] = constants.TAB_DASHBOARD;
      paramNamesToDefaultValuesMap[this.DASHBOARD_MODE] = constants.DASHBOARD_MODE_METRIC_SUMMARY;
      paramNamesToDefaultValuesMap[this.DASHBOARD_DASHBOARD_NAME] = undefined;
      paramNamesToDefaultValuesMap[this.DASHBOARD_SUMMARY_DASHBOARD_ID] = undefined;
      this.controllerNameToParamNamesMap[this.DASHBOARD_METRIC_SUMMARY_CONTROLLER] = paramNamesToDefaultValuesMap;

      // dashboard wowSummary Controller
      paramNamesToDefaultValuesMap = {};
      paramNamesToDefaultValuesMap[this.TAB] = constants.TAB_DASHBOARD;
      paramNamesToDefaultValuesMap[this.DASHBOARD_MODE] = constants.DASHBOARD_MODE_WOW_SUMMARY;
      paramNamesToDefaultValuesMap[this.DASHBOARD_DASHBOARD_NAME] = undefined;
      paramNamesToDefaultValuesMap[this.DASHBOARD_SUMMARY_DASHBOARD_ID] = undefined;
      this.controllerNameToParamNamesMap[this.DASHBOARD_WOW_SUMMARY_CONTROLLER] = paramNamesToDefaultValuesMap;


      // anomaliesController
      paramNamesToDefaultValuesMap = {};
      paramNamesToDefaultValuesMap[this.TAB] = constants.TAB_ANOMALIES;
      paramNamesToDefaultValuesMap[this.ANOMALIES_ANOMALIES_SEARCH_MODE] = constants.MODE_TIME;
      paramNamesToDefaultValuesMap[this.ANOMALIES_START_DATE] = moment().subtract(1, 'days').startOf('day').valueOf();
      paramNamesToDefaultValuesMap[this.ANOMALIES_END_DATE] = moment().subtract(0, 'days').startOf('day').valueOf();;
      paramNamesToDefaultValuesMap[this.ANOMALIES_PAGE_NUMBER] = 1;
      paramNamesToDefaultValuesMap[this.ANOMALIES_METRIC_IDS] = undefined;
      paramNamesToDefaultValuesMap[this.ANOMALIES_DASHBOARD_ID] = undefined;
      paramNamesToDefaultValuesMap[this.ANOMALIES_ANOMALY_IDS] = undefined;
      this.controllerNameToParamNamesMap[this.ANOMALIES_CONTROLLER] = paramNamesToDefaultValuesMap;

      // analysis


      console.log('hash Params init');
      console.log(this.controllerNameToParamNamesMap);
    }
}

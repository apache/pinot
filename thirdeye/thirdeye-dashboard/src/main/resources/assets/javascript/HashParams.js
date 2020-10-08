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
  this.ANOMALIES_GROUP_IDS = 'anomalyGroupIds';
  this.ANOMALIES_SEARCH_FILTERS = 'searchFilters';
  this.INVESTIGATE_ANOMALY_ID = 'anomalyId';

  this.ANALYSIS_METRIC_ID = 'metricId';
  this.ANALYSIS_CURRENT_START= 'currentStart';
  this.ANALYSIS_CURRENT_END = 'currentEnd';
  this.ANALYSIS_BASELINE_START = 'baselineStart';
  this.ANALYSIS_BASELINE_END = 'baselineEnd';
  this.ANALYSIS_FILTERS = 'filters';
  this.ANALYSIS_GRANULARITY = 'granularity';
  this.ANALYSIS_DIMENSION = 'dimension';
  this.ANALYSIS_COMPARE_MODE = 'compareMode';

  this.HEATMAP_CURRENT_START = 'heatMapCurrentStart';
  this.HEATMAP_CURRENT_END = 'heatMapCurrentEnd';
  this.HEATMAP_BASELINE_START = 'heatMapBaselineStart';
  this.HEATMAP_BASELINE_END = 'heatMapBaselineEnd';
  this.HEATMAP_MODE = 'heatmapMode';
  this.HEATMAP_FILTERS = 'heatMapFilters';

  this.RAND = 'rand';

  // this map has key = <controller name> and value = <map of param names and its default value>
  this.controllerNameToParamNamesMap = {};
  this.APP_CONTROLLER = 'app';
  this.DASHBOARD_CONTROLLER = 'dashboard';
  this.ANOMALIES_CONTROLLER = 'anomalies';
  this.ANALYSIS_CONTROLLER = 'analysis';
  this.INVESTIGATE_CONTROLLER = 'investigate';
  this.DASHBOARD_ANOMALY_SUMMARY_CONTROLLER = 'anomalySummary';
  this.DASHBOARD_METRIC_SUMMARY_CONTROLLER = 'metricSummary';
  this.DASHBOARD_WOW_SUMMARY_CONTROLLER = 'wowSummary';

}


HashParams.prototype = {
    init() {
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
      paramNamesToDefaultValuesMap[this.ANOMALIES_START_DATE] = undefined;
      paramNamesToDefaultValuesMap[this.ANOMALIES_END_DATE] = undefined;
      paramNamesToDefaultValuesMap[this.ANOMALIES_PAGE_NUMBER] = 1;
      paramNamesToDefaultValuesMap[this.ANOMALIES_METRIC_IDS] = null;
      paramNamesToDefaultValuesMap[this.ANOMALIES_DASHBOARD_ID] = null;
      paramNamesToDefaultValuesMap[this.ANOMALIES_GROUP_IDS] = null;
      paramNamesToDefaultValuesMap[this.ANOMALIES_ANOMALY_IDS] = null;
      paramNamesToDefaultValuesMap[this.ANOMALIES_SEARCH_FILTERS] = null;
      this.controllerNameToParamNamesMap[this.ANOMALIES_CONTROLLER] = paramNamesToDefaultValuesMap;

      // analysis
      paramNamesToDefaultValuesMap = {};
      paramNamesToDefaultValuesMap[this.TAB] = constants.TAB_ANALYSIS;
      paramNamesToDefaultValuesMap[this.ANALYSIS_CURRENT_START] = null;
      paramNamesToDefaultValuesMap[this.ANALYSIS_CURRENT_END]= null;
      paramNamesToDefaultValuesMap[this.ANALYSIS_METRIC_ID]= null;
      paramNamesToDefaultValuesMap[this.ANALYSIS_BASELINE_START] = null;
      paramNamesToDefaultValuesMap[this.ANALYSIS_BASELINE_END] = null;
      paramNamesToDefaultValuesMap[this.ANALYSIS_FILTERS] = null;
      paramNamesToDefaultValuesMap[this.ANALYSIS_GRANULARITY] = null;
      paramNamesToDefaultValuesMap[this.ANALYSIS_DIMENSION] = constants.DEFAULT_ANALYSIS_DIMENSION;
      paramNamesToDefaultValuesMap[this.ANALYSIS_COMPARE_MODE] = constants.DEFAULT_COMPARE_MODE;

      paramNamesToDefaultValuesMap[this.HEATMAP_CURRENT_START] = null;
      paramNamesToDefaultValuesMap[this.HEATMAP_CURRENT_END] = null;
      paramNamesToDefaultValuesMap[this.HEATMAP_BASELINE_START] = null;
      paramNamesToDefaultValuesMap[this.HEATMAP_BASELINE_END] = null;
      paramNamesToDefaultValuesMap[this.HEATMAP_MODE] = null;
      paramNamesToDefaultValuesMap[this.HEATMAP_FILTERS] = null;


      this.controllerNameToParamNamesMap[this.ANALYSIS_CONTROLLER] = paramNamesToDefaultValuesMap;

      // investigate
      paramNamesToDefaultValuesMap = {};
      paramNamesToDefaultValuesMap[this.TAB] = constants.TAB_INVESTIGATE;
      paramNamesToDefaultValuesMap[this.INVESTIGATE_ANOMALY_ID] = null;
      this.controllerNameToParamNamesMap[this.INVESTIGATE_CONTROLLER] = paramNamesToDefaultValuesMap;
    },

    isSame(key, currentValue, newValue) {
      switch(key){
        case this.ANALYSIS_CURRENT_START:
        case this.ANALYSIS_CURRENT_END :
        case this.ANALYSIS_BASELINE_START :
        case this.ANALYSIS_BASELINE_END :
        case this.ANOMALIES_START_DATE :
        case this.ANOMALIES_END_DATE:
        case this.ANALYSIS_BASELINE_END:
        case this.HEATMAP_CURRENT_START:
        case this.HEATMAP_CURRENT_END:
        case this.HEATMAP_BASELINE_START:
        case this.HEATMAP_BASELINE_END:
          return currentValue.isSame(newValue, 'day');

        case this.ANALYSIS_FILTERS:
        case this.HEATMAP_FILTERS:
          return JSON.stringify(currentValue) === newValue;

        default:
          return currentValue == newValue;
      }
    },

};

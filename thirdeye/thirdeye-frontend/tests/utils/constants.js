import { isPresent } from "@ember/utils";

/**
 * General self-serve element selectors
 */
export const selfServeConst = {
  METRIC_SELECT: '#select-metric',
  METRIC_INPUT: '.ember-power-select-search-input',
  GRANULARITY_SELECT: '#select-granularity',
  DIMENSION_SELECT: '#select-dimension',
  PATTERN_SELECT: '#select-pattern',
  FILTER_SELECT: '#select-filters',
  SUBGROUP_SELECT: '#config-group',
  INPUT_NAME: '#anomaly-form-function-name',
  GRAPH_CONTAINER: '.te-graph-alert',
  OPTION_LIST: '.ember-power-select-options',
  OPTION_ITEM: 'li.ember-power-select-option',
  SELECTED_ITEM: '.ember-power-select-selected-item',
  APP_OPTIONS: '#anomaly-form-app-name',
  SPINNER: '.spinner-display',
  CONFIG_GROUP_ALERTS: '.panel-title',
  CONFIG_BLOCK: '.te-form__section-config',
  CONFIG_RECIPIENTS_INPUT: '#config-group-add-recipients',
  EMAIL_WARNING: '.te-form__alert--warning',
  SUBMIT_BUTTON: '.te-button--submit',
  RESET_BUTTON: '.te-button--cancel',
  SECONDARY_LINK: '.thirdeye-link-secondary',
  IMPORT_DASHBOARD_INPUT: '#existing-dashboard-name',
  IMPORT_DATASET_INPUT: '#import-custom-dataset',
  IMPORT_METRIC_INPUT: '#import-custom-metric',
  IMPORT_CONSOLIDATE_INPUT: '#select-consolidate',
  IMPORT_RRD_INPUT: '#import-custom-rrd',
  IMPORT_WARNING: '.alert-warning',
  IMPORT_SUCCESS: '.te-form__banner--success',
  IMPORT_RESULT_LIST: '.te-form__banner-list',
  PATTERN_OPTIONS: ['Higher or lower than expected', 'Higher than expected', 'Lower than expected']
};

/**
 * General root-cause element selectors
 */
export const rootCauseConst = {
  PLACEHOLDER: '.rootcause-placeholder',
  TABS: '.rootcause-tabs',
  LABEL: '.rootcause-legend__label',
  SELECTED_METRIC: '.rootcause-select-metric-dimension',
  ROOTCAUSE_HEADER: 'rootcause-header',
  HEADER: '.rootcause-header__major',
  LAST_SAVED: '.rootcause-header__last-updated-info',
  COMMENT_TEXT: '.rootcause-header--textarea',
  BASELINE: '#select-compare-mode',
  EXPAND_ANOMALY_BTN: '.rootcause-anomaly__icon a',
  ANOMALY_TITLE: '.rootcause-anomaly__title',
  ANOMALY_VALUE: '.rootcause-anomaly__props-value',
  ANOMALY_STATUS: '.ember-radio-button.checked',
  SAVE_BTN: '.te-button',
  METRICS_TABLE: '.rootcause-metrics',
  HEATMAP_DROPDOWN: '#select-heatmap-mode',
  SELECTED_HEATMAP_MODE: '.ember-power-select-selected-item',
  EVENTS_FILTER_BAR: '.filter-bar',
  EVENTS_TABLE: '.events-table',
  RCA_TOGGLE: '.rootcause-to-legacy-toggle'
};

/**
 * Converts list options to comma-separated string
 */
export const optionsToString = ($optionList) => {
  return Object.values($optionList).mapBy('innerText').filter(isPresent).join();
};

export default {
  selfServeConst,
  rootCauseConst,
  optionsToString
};

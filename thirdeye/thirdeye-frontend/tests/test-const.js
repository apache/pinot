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
  PATTERN_OPTIONS: ['Higher or lower than expected', 'Higher than expected', 'Lower than expected']
};

/**
 * Converts list options to comma-separated string
 */
export const optionsToString = ($optionList) => {
  return Object.values($optionList).mapBy('innerText').filter(isPresent).join();
};

export default {
  selfServeConst,
  optionsToString
};

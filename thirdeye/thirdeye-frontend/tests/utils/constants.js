import { isPresent } from '@ember/utils';

/**
 * General self-serve element selectors
 */
export const selfServeConst = {
  // Self-serve Nav Elements
  NAV_MANAGE_ALERTS: '.te-nav__link:contains("Alerts")',

  // Onboard Alert Elements
  ALERT_NAME_INPUT: '#anomaly-form-function-name',
  SUBSCRIPTION_GROUP: '#anomaly-form-app-name',
  STATUS: '.te-toggle--form span',
  STATUS_RESULT: '.te-search-results__tag',
  EDIT_LINK: '/manage/alert/edit',
  STATUS_TOGGLER: '.x-toggle-btn',
  NEW_FUNC_NAME: 'test_function_2',
  NEW_FUNC_RESULT: '.te-search-results__title-name',
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
  SECONDARY_LINK_GREY: '.thirdeye-link-secondary-grey',
  IMPORT_DASHBOARD_INPUT: '#existing-dashboard-name',
  IMPORT_DATASET_INPUT: '#import-custom-dataset',
  IMPORT_METRIC_INPUT: '#import-custom-metric',
  IMPORT_CONSOLIDATE_INPUT: '#select-consolidate',
  IMPORT_RRD_INPUT: '#import-custom-rrd',
  IMPORT_WARNING: '.alert-warning',
  IMPORT_SUCCESS: '.te-form__banner--success',
  IMPORT_RESULT_LIST: '.te-form__banner-list',
  ALERT_ACTIVE_LABEL: '.te-search-results__tag--active',
  ALERT_TITLE: '.te-search-results__title',
  PATTERN_OPTIONS: ['Higher or lower than expected', 'Higher than expected', 'Lower than expected'],

  // Alert search page elements: /manage/alerts
  RESULTS_TITLE: '.te-search-title',
  RESULTS_LINK: '.te-search-results__title-name',
  ALERT_PROPS_ITEM: '.te-search-results__option',

  // Alert Page elements
  ALERT_CARDS_CONTAINER: '.te-horizontal-cards__container',
  ALERT_PAGE_HEADER_PROPS: '.te-search-results__list--details-block',
  ALERT_PAGE_HEADER_LINK: '.te-search-results__cta',
  RANGE_PILL_SELECTOR_LIST: '.range-pill-selectors__list',
  RANGE_PILL_SELECTOR_ITEM: '.range-pill-selectors__item',
  RANGE_PILL_SELECTOR_ACTIVE: '.range-pill-selectors__item--active',
  RANGE_PILL_SELECTOR_TRIGGER: '.range-pill-selectors__range-picker .daterangepicker-input',
  RANGE_PILL_PRESET_OPTION: '.daterangepicker:last .ranges li:contains("Last 1 month")',
  RANGE_PICKER_INPUT: '.daterangepicker-input',
  RANGE_PICKER_PRESETS: '.daterangepicker.show-calendar .ranges ul li',
  LINK_TUNE_ALERT: '.te-self-serve__side-link:contains("Customize sensitivity")',

  // Tuning Page Elements
  LINK_ALERT_PAGE: '.te-button:contains("Back to overview")'

};

/**
 * General root-cause element selectors
 */
export const rootCauseConst = {
  PLACEHOLDER: '.rootcause-placeholder',
  TABS: '.common-tabs',
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
  DIMENSIONS_TABLE: '.rootcause-dimensions',
  METRICS_TABLE: '.rootcause-metrics',
  HEATMAP_DROPDOWN: '#select-heatmap-mode',
  SELECTED_HEATMAP_MODE: '.ember-power-select-selected-item',
  EVENTS_FILTER_BAR: '.filter-bar',
  EVENTS_TABLE: '.events-table',
  RCA_TOGGLE: '.rootcause-to-legacy-toggle'
};

/**
 * General or global element selectors
 */
export const generalConst = {
  MODAL: '.te-modal',
  MICRO_MODAL: '.te-modal-micro',
  MODAL_TITLE: '.te-modal__title'
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
  generalConst,
  optionsToString
};

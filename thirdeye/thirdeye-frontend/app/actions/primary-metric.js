import { hash } from 'rsvp';
import { type } from './utils';
import fetch from 'fetch';
import moment from 'moment';
import { COMPARE_MODE_MAPPING } from './constants';

/**
 * Define the metric action types
 */
export const ActionTypes = {
  LOADING: type('[Primary Metric] Loading'),
  REQUEST_FAIL: type('[Primary Metric] Request Fail'),
  LOAD_IDS: type('[Primary Metric] Load related Primary Metric Ids'),
  LOAD_DATA: type('[Primary Metric] Load related Primary Metric Data'),
  LOAD_REGIONS: type('[Primary Metric] Load Primary Metric Regions'),
  LOAD_PRIMARY_METRIC: type('[Primary Metric] Load Primary Primary Metric'),
  UPDATE_COMPARE_MODE: type('[Primary Metric] Update Compare Mode'),
  UPDATE_DATES: type('[Primary Metric] Update Date'),
  SELECT_PRIMARY: type('[Primary Metric] Toggles Primary Metric Selection'),
  SELECT_DIMENSION: type('[Primary Metric] Set Selected Dimension'),
  SELECT_EVENTS: type('[Primary Metric] Set Selected Events'),
  SELECT_METRICS: type('[Primary Metric] set Selected Metrics'),
  RESET: type('[Primary Metric] Reset Selected Data')
};


function loading() {
  return {
    type: ActionTypes.LOADING
  };
}

function requestFail() {
  return {
    type: ActionTypes.REQUEST_FAIL
  };
}

function loadRelatedMetricsData(response) {
  return {
    type: ActionTypes.LOAD_DATA,
    payload: response
  };
}

function loadRegions(response) {
  return {
    type: ActionTypes.LOAD_REGIONS,
    payload: response
  };
}

function setPrimaryMetricData(response) {
  return {
    type: ActionTypes.LOAD_PRIMARY_METRIC,
    payload: response
  };
}

function updateCompareMode(response) {
  return {
    type: ActionTypes.UPDATE_COMPARE_MODE,
    payload: response
  };
}

function updateDates(response) {
  return {
    type: ActionTypes.UPDATE_DATES,
    payload: response
  };
}

function setSelectedDimension(response) {
  return {
    type: ActionTypes.SELECT_DIMENSION,
    payload: response
  };
}

function setSelectedEvent(response) {
  return {
    type: ActionTypes.SELECT_EVENTS,
    payload: response
  };
}

function setSelectedMetrics(response) {
  return {
    type: ActionTypes.SELECT_METRICS,
    payload: response
  };
}

function resetSelectedData() {
  return {
    type: ActionTypes.RESET
  };
}

function selectPrimary() {
  return {
    type: ActionTypes.SELECT_PRIMARY
  };
}


/**
 * Initialize store with metric data from query params
 * @param {Object} metric
 */
function setPrimaryMetric(metric) {
  return (dispatch) => {
    dispatch(setPrimaryMetricData(metric));
    return Promise.resolve();
  };
}

/**
 * Fetches anomaly regions for metrics
 */
function fetchRegions() {
  return (dispatch, getState) => {
    const store = getState();
    const {
      primaryMetricId,
      relatedMetricIds,
      filters,
      currentStart,
      currentEnd
    } = store.primaryMetric;

    const metricIds = [primaryMetricId, ...relatedMetricIds].join(',');
    // todo: identify better way for query params
    return fetch(`/data/anomalies/ranges?metricIds=${metricIds}&start=${currentStart}&end=${currentEnd}&filters=${encodeURIComponent(filters)}`)
      .then(res => res.json())
      .then(res => dispatch(loadRegions(res)))
      .catch(() => {
        dispatch(requestFail());
      });

  };
}

/**
 * Redux Thunk that fetches the data for related Metrics
 */
function fetchRelatedMetricData() {
  return (dispatch, getState) => {
    const store = getState();
    const {
      primaryMetricId,
      filters,
      granularity,
      currentStart,
      currentEnd,
      compareMode
    } = store.primaryMetric;


    const offset = COMPARE_MODE_MAPPING[compareMode] || 1;
    const metricIds = [primaryMetricId];
    const baselineStart = moment(+currentStart).subtract(offset, 'week').valueOf();
    const baselineEnd = moment(+currentEnd).subtract(offset, 'week').valueOf();

    if (!metricIds.length) { return; }
    const promiseHash = metricIds.reduce((hash, id) => {
      const url = `/timeseries/compare/${id}/${currentStart}/${currentEnd}/${baselineStart}/${baselineEnd}?dimension=All&granularity=${granularity}&filters=${encodeURIComponent(filters)}`;
      hash[id] = fetch(url).then(res => res.json());

      return hash;
    }, {});

    return hash(promiseHash)
      .then(res => dispatch(loadRelatedMetricsData(res)))
      .catch(() => {
        dispatch(requestFail());
      });
  };
}

/**
 * Updates the date range for the primary metric
 * @param {Number} startDate The start time in unix ms
 * @param {Number} endDate The end time in unix ms
 */
function updateMetricDate(startDate, endDate) {
  return (dispatch) => {
    startDate = startDate ? moment(Number(startDate)) : startDate;
    endDate = endDate ? moment(Number(endDate)) : endDate;

    dispatch(updateDates({
      analysisStart: startDate.valueOf(),
      analysisEnd: endDate.valueOf()
    }));
  };
}

/**
 * Updates the anallysis date range for the primary metric
 * @param {Number} startDate The start time in unix ms
 * @param {Number} endDate The end time in unix ms
 */
function updateAnalysisDates(startDate, endDate) {
  return (dispatch) => {
    dispatch(updateMetricDate(startDate, endDate));
  };
}

/**
 * Sets the subdimension as selected
 * @param {Object} subdimension The newly selected subdimension object
 */
function selectDimension(subdimension) {
  return (dispatch) => {
    const name = `${subdimension.dimension}-${subdimension.name}`;

    return dispatch(setSelectedDimension(name));
  };
}

/**
 * Sets the event as selected
 * @param {String} name The name of the newly selected event
 */
function selectEvent(name) {
  return (dispatch) => {
    return dispatch(setSelectedEvent(name));
  };
}

/**
 * Sets the metric as selected
 * @param {String} metric The name of the newly selected event
 */
function selectMetric(metric) {
  return (dispatch) => {
    return dispatch(setSelectedMetrics(metric));
  };
}

// Resets the store to its initial state
function reset() {
  return (dispatch) => {
    dispatch(resetSelectedData());
    return Promise.resolve();
  };
}

export const Actions = {
  loading,
  requestFail,
  fetchRelatedMetricData,
  fetchRegions,
  setPrimaryMetric,
  updateCompareMode,
  updateMetricDate,
  updateAnalysisDates,
  selectPrimary,
  selectMetric,
  selectDimension,
  selectEvent,
  reset
};


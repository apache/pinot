import { type } from './utils';
import fetch from 'fetch';
import Ember from 'ember';
import moment from 'moment'
/**
 * Define the metric action types 
 */
export const ActionTypes = {
  LOADING: type('[Metric] Loading'),
  REQUEST_FAIL: type('[Metric] Request Fail'),
  LOAD_IDS: type('[Metric] Load related Metric Ids'),
  LOAD_DATA: type('[Metric] Load related Metric Data'),
  LOAD_REGIONS: type('[Metric] Load Metric Regions'),
  LOAD_PRIMARY_METRIC: type('[Metric] Load Primary Metric'),
  UPDATE_COMPARE_MODE: type('[Metric] Update Compare Mode'),
};

// Todo: move this in a constant.js file
const COMPARE_MODE_MAPPING = {
  WoW: 1,
  Wo2W: 2,
  Wo3W: 3,
  Wo4W: 4
}

function loading() {
  return {
    type: ActionTypes.LOADING
  };
}

function requestFail() {
  return {
    type: ActionTypes.REQUEST_FAIL,
  };
}

function loadRelatedMetricIds(response) {
  return {
    type: ActionTypes.LOAD_IDS,
    payload: response
  }
}

function loadRelatedMetricsData(response) {
  return {
    type: ActionTypes.LOAD_DATA,
    payload: response
  }
}

function loadRegions(response) {
  return {
    type: ActionTypes.LOAD_REGIONS,
    payload: response
  }
}

function setPrimaryMetricData(response) {
  return {
    type: ActionTypes.LOAD_PRIMARY_METRIC,
    payload: response
  }
}

function updateCompareMode(response) {
  return {
    type: ActionTypes.UPDATE_COMPARE_MODE,
    payload: response
  }
}

/**
 * Get all related metric's id for the primary metric
 */
function fetchRelatedMetricIds() {
  return (dispatch, getState) => {
    dispatch(loading());
    const store = getState();

    let {
      primaryMetricId: metricId, 
      currentStart: startDate, 
      currentEnd: endDate
    } = store.metrics;

    endDate = endDate || moment().subtract(1, 'day').endOf('day').valueOf();
    startDate = startDate || moment(endDate).subtract(1, 'week').valueOf();

    const baselineStart = moment(startDate).subtract(1, 'week').valueOf();
    const windowSize = Math.max(endDate - startDate, 0);
 
    if (!metricId) {
      return Promise.reject(new Error("Must provide a metricId"));
    }
    // todo: identify better way for query params
    return fetch(`/rootcause/queryRelatedMetrics?current=${startDate}&baseline=${baselineStart}&windowSize=${windowSize}&metricUrn=thirdeye:metric:${metricId}`)
      .then(res => res.json())
      .then(res => dispatch(loadRelatedMetricIds(res)))
      .catch(() => {
        // Todo: dispatch an error message
      })
  }
}

/**
 * Initialize store with metric data from query params
 * @param {*} metric 
 */
function setPrimaryMetric(metric) {
  return (dispatch) => {
    dispatch(setPrimaryMetricData(metric));
    return Promise.resolve();
  }
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
    } = store.metrics;

    const metricIds = [primaryMetricId, ...relatedMetricIds].join(',');
     // todo: identify better way for query params
    return fetch(`/data/anomalies/ranges?metricIds=${metricIds}&start=${currentStart}&end=${currentEnd}&filters=${filters}`)
      .then(res => res.json())
      .then(res => dispatch(loadRegions(res)))
      .catch(() => {
        // Todo: dispatch an error message
      })

  }
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
      relatedMetricIds,
      compareMode
    } = store.metrics;


    const offset = COMPARE_MODE_MAPPING[compareMode] || 1;
    const metricIds = [primaryMetricId, ...relatedMetricIds];
    const baselineStart = moment(currentStart).subtract(offset, 'week').valueOf();
    const baselineEnd = moment(currentEnd).subtract(offset, 'week').valueOf();

    if (!metricIds.length) { return; }
    const promiseHash = metricIds.reduce((hash,id) => {
      const url = `/timeseries/compare/${id}/${currentStart}/${currentEnd}/${baselineStart}/${baselineEnd}?dimension=All&granularity=${granularity}&filters=${filters}`
      hash[id] = fetch(url).then(res => res.json());

      return hash;
    }, {})

    return Ember.RSVP.hash(promiseHash)
      .then(res => dispatch(loadRelatedMetricsData(res)))
      .then()
      .catch(() => {
        // Todo: dispatch an error message
      })
  }
}

export const Actions = {
  loading,
  requestFail,
  fetchRelatedMetricData,
  fetchRelatedMetricIds,
  fetchRegions,
  setPrimaryMetric,
  updateCompareMode,
};


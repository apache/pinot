import { type } from './utils';
import fetch from 'fetch';
import Ember from 'ember';
import moment from 'moment';
import _ from 'lodash';
import {
  COMPARE_MODE_MAPPING,
  colors,
  ROOTCAUSE_ANALYSIS_DURATION_MAX,
  ROOTCAUSE_ANOMALY_DURATION_MAX
} from './constants';

/**
 * Define the metric action types
 */
export const ActionTypes = {
  LOADING: type('[Metric] Loading'),
  LOADED: type('[Metric] Loaded'),
  REQUEST_FAIL: type('[Metric] Request Fail'),
  LOAD_IDS: type('[Metric] Load related Metric Ids'),
  LOAD_DATA: type('[Metric] Load related Metric Data'),
  LOAD_REGIONS: type('[Metric] Load Metric Regions'),
  LOAD_PRIMARY_METRIC: type('[Metric] Load Primary Metric'),
  UPDATE_COMPARE_MODE: type('[Metric] Update Compare Mode'),
  UPDATE_DATE: type('[Metric] Update Date'),
  SET_DATE: type('[Metric] Set new region dates'),
  SELECT_METRIC: type('[Metric] Set Selected Metric'),
  RESET: type('[Metric] Reset Data')
};

/**
 * Determines if a metric should be filtered out
 * @param {Object} metric
 * @returns {Boolean}
 */
const filterMetric = (metric) => {
  return metric
  && metric.subDimensionContributionMap['All'].currentValues
  && metric.subDimensionContributionMap['All'].currentValues.reduce((total, val) => {
    return total + val;
  }, 0);
};

function loading() {
  return {
    type: ActionTypes.LOADING
  };
}

/**
 * Set Metrics Status to loaded
 */
function loaded() {
  return {
    type: ActionTypes.LOADED
  };
}

function requestFail() {
  return {
    type: ActionTypes.REQUEST_FAIL
  };
}

function loadRelatedMetricIds(response) {
  return {
    type: ActionTypes.LOAD_IDS,
    payload: response
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

function updateDate(response) {
  return {
    type: ActionTypes.UPDATE_DATE,
    payload: response
  };
}

function resetData() {
  return {
    type: ActionTypes.RESET
  };
}

function setDate(dates) {
  return {
    type: ActionTypes.SET_DATE,
    payload: dates
  };
}

/**
 * Get all related metric's id for the primary metric
 */
function fetchRelatedMetricIds() {
  return (dispatch, getState) => {
    dispatch(loading());
    const { metrics, primaryMetric } = getState();

    let {
      primaryMetricId: metricId,
      currentStart: analysisStartRaw,
      currentEnd: analysisEndRaw,
      regionStart: anomalyStartRaw,
      regionEnd: anomalyEndRaw
    } = metrics;

    const {
      compareMode,
      filters: filterJson
    } = primaryMetric;

    const analysisDurationSafe = Math.min(analysisEndRaw - analysisStartRaw, ROOTCAUSE_ANALYSIS_DURATION_MAX);
    const analysisStart = Math.max(analysisStartRaw, analysisEndRaw - analysisDurationSafe);
    const analysisEnd = analysisEndRaw;

    const anomalyDurationSafe = Math.min(anomalyEndRaw - anomalyStartRaw, ROOTCAUSE_ANOMALY_DURATION_MAX);
    const anomalyStart = anomalyStartRaw;
    const anomalyEnd = anomalyStartRaw + anomalyDurationSafe;

    const offset = COMPARE_MODE_MAPPING[compareMode] || 1;
    const baselineStart = moment(anomalyStart).subtract(offset, 'week').valueOf();
    const baselineEnd = moment(anomalyEnd).subtract(offset, 'week').valueOf();

    if (!metricId) {
      return Promise.reject(new Error("Must provide a metricId"));
    }

    const filters = JSON.parse(filterJson);
    const filterUrns = Object.keys(filters).map(key => 'thirdeye:dimension:' + key + ':' + filters[key] + ':provided');
    const primaryUrn = `thirdeye:metric:${metricId}`;

    const urns = [primaryUrn].concat(filterUrns).join(',');

    return fetch(`/rootcause/query?framework=relatedMetrics&anomalyStart=${anomalyStart}&anomalyEnd=${anomalyEnd}&baselineStart=${baselineStart}&baselineEnd=${baselineEnd}&analysisStart=${analysisStart}&analysisEnd=${analysisEnd}&urns=${urns}`)
      .then(res => res.json())
      .then(res => res.filter(metric => metric.urn != primaryUrn))
      .then(res => dispatch(loadRelatedMetricIds(res)));
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
    } = store.metrics;

    const metricIds = [primaryMetricId, ...relatedMetricIds].join(',');

    return fetch(`/data/anomalies/ranges?metricIds=${metricIds}&start=${currentStart}&end=${currentEnd}&filters=${encodeURIComponent(filters)}`)
      .then(res => res.json())
      .then(res => dispatch(loadRegions(res)));
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
      relatedMetricIds,
      compareMode
    } = store.metrics;

    const offset = COMPARE_MODE_MAPPING[compareMode] || 1;
    const metricIds = [primaryMetricId, ...relatedMetricIds];
    const baselineStart = moment(currentStart).subtract(offset, 'week').valueOf();
    const baselineEnd = moment(currentEnd).subtract(offset, 'week').valueOf();

    if (!metricIds.length) { return; }
    const promiseHash = metricIds.reduce((hash, id) => {
      const url = `/timeseries/compare/${id}/${currentStart}/${currentEnd}/${baselineStart}/${baselineEnd}?dimension=All&granularity=${granularity}&filters=${encodeURIComponent(filters)}`;
      hash[id] = fetch(url).then(res => res.json());

      return hash;
    }, {});

    return Ember.RSVP.hash(promiseHash)
      .then((metrics) => {
        const filteredMetrics = _.pickBy(metrics, filterMetric);

        metricIds.forEach((id, index) => {
          const filter = filteredMetrics[id];
          if (filter) {
            filter.color = colors[index % colors.length];
          }
        });
        return filteredMetrics;
      })
      .then(res => dispatch(loadRelatedMetricsData(res)))
      .catch(() => {});
  };
}

/**
 * Updates the date range for the correlated metrics
 * @param {Number} startDate The start time in unix ms
 * @param {Number} endDate The end time in unix ms
 */
function updateMetricDate(startDate, endDate) {
  return (dispatch, getState) => {
    const store = getState();
    const {
      currentStart,
      currentEnd
    } = store.metrics;
    startDate = moment(startDate);
    endDate = moment(endDate);

    const shouldUpdateStart = startDate.isBefore(currentStart);
    const shouldUpdateEnd = endDate.isAfter(currentEnd);


    if (shouldUpdateStart && !shouldUpdateEnd) {
      const newStartDate = currentStart - (currentEnd - currentStart) ;

      dispatch(updateDate({
        currentStart: newStartDate,
        currentEnd
      }));

      return Promise.resolve();
    }
  };
}

/**
 * Updates the date range for the dimensions and refetches the data
 * @param {Number} start The start time in unix ms
 * @param {Number} end The end time in unix ms
 */
function updateDates(start, end) {
  return (dispatch) => {

    return dispatch(setDate([start, end]));
  };
}

// Resets the store to its initial state
function reset() {
  return (dispatch) => {
    dispatch(resetData());
    return Promise.resolve();
  };
}

export const Actions = {
  loading,
  loaded,
  requestFail,
  fetchRelatedMetricData,
  fetchRelatedMetricIds,
  fetchRegions,
  setPrimaryMetric,
  updateCompareMode,
  updateMetricDate,
  reset,
  updateDates
};


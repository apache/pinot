import { type } from './utils';
import { COMPARE_MODE_MAPPING } from './constants';
import fetch from 'fetch';
import moment from 'moment';

/**
 * Define the anomaly action types
 */
export const ActionTypes = {
  LOAD: type('[Dimensions] Load'),
  LOADING: type('[Dimensions] Loading'),
  LOADED: type('[Dimensions] Loaded'),
  LOAD_TIMESERIES: type('[Dimensions] Load TimeSeries'),
  LOAD_HEATMAP: type('[Dimensions] Load HeatMap'),
  SET: type('[Dimension] Set Dimension'),
  SET_DATE: type('[Dimension] Set new region dates'),
  RESET: type('[Dimensions] Reset Data'),
  REQUEST_FAIL: type('[Dimensions] Request Fail')
};

function resetData() {
  return {
    type: ActionTypes.RESET
  };
}

/**
 * Set Dimension Status to loaded
 */
function loaded() {
  return {
    type: ActionTypes.LOADED
  };
}

function loading() {
  return {
    type: ActionTypes.LOADING
  };
}

function setDimension(dimension = 'All') {
  return {
    type: ActionTypes.SET,
    payload: dimension
  };
}

function loadTimeSeries(response) {
  return {
    type: ActionTypes.LOAD_TIMESERIES,
    payload: response
  };
}

function loadHeatMap(response) {
  return {
    type: ActionTypes.LOAD_HEATMAP,
    payload: response
  };
}

function load(response, metricId) {
  return {
    type: ActionTypes.LOAD,
    payload: response,
    metricId
  };
}

function requestFail() {
  return {
    type: ActionTypes.REQUEST_FAIL
  };
}

function setDate(dates) {
  return {
    type: ActionTypes.SET_DATE,
    payload: dates
  };
}


/**
* Fetches the dimensions data for a specific metric
* @param {Number} metricId
*/
function fetchDimensions(metricId) {
  return (dispatch, getState) => {
    const { dimensions } = getState();

    if (dimensions.metricId === metricId) {
      return {};
    }

    // TODO: save url in an API folder
    // need to have a new endpoint with just the anomaly details
    return fetch(`/data/autocomplete/dimensions/metric/${metricId}`)
      .then(res => res.json())
      .then(res => dispatch(load(res, metricId)))
      .catch((res) => dispatch(requestFail(res)));
  };
}

/**
 * Fetches subdimensions and heatmap Data for a dimension
 * @param {String} newDimension
 */
function updateDimension(newDimension) {
  return (dispatch, getState) => {
    const { primaryMetric, dimensions } = getState();
    const {
      granularity,
      compareMode,
      currentStart,
      currentEnd,
      primaryMetricId,
      filters
    } = primaryMetric;

    const offset = COMPARE_MODE_MAPPING[compareMode] || 1;
    const baselineStart = moment(+currentStart).subtract(offset, 'week').valueOf();
    const baselineEnd = moment(+currentEnd).subtract(offset, 'week').valueOf();
    newDimension = newDimension || dimensions.selectedDimension;

    const url = `/timeseries/compare/${primaryMetricId}/${currentStart}/${currentEnd}/${baselineStart}/${baselineEnd}?dimension=${newDimension}&filters=${encodeURIComponent(filters)}&granularity=${granularity}`;

    dispatch(setDimension(newDimension));
    return fetch(url)
      .then(res => res.json())
      .then(res => dispatch(loadTimeSeries(res)));
  };
}

/**
 * Fetches heatmap Data  for a metric
 * @param {Number} start The start time in unix ms
 * @param {Number} end The end time in unix ms
 */
function fetchHeatMapData(start, end) {
  return (dispatch, getState) => {
    const { primaryMetric, dimensions } = getState();
    const {
      primaryMetricId,
      filters,
      compareMode
    } = primaryMetric;

    if(!(start && end && primaryMetricId)) { return; }

    const offset = COMPARE_MODE_MAPPING[compareMode] || 1;
    const baselineStart = moment(+start).subtract(offset, 'week').valueOf();
    const baselineEnd = moment(+end).subtract(offset, 'week').valueOf();

    const heatmapUrl = `/data/heatmap/${primaryMetricId}/${start}/${end}/${baselineStart}/${baselineEnd}?filters=${encodeURIComponent(filters)}`;

    return fetch(heatmapUrl)
      .then(res => res.json())
      .then(res => dispatch(loadHeatMap(res)));
  };
}

/**
 * Updates the date range for the dimensions and refetches the data
 * @param {Number} start The start time in unix ms
 * @param {Number} end The end time in unix ms
 */
function updateDates(start, end) {
  return (dispatch) => {
    // const { primaryMetric } = getState();
    //check if dates are stame

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
  load,
  loaded,
  requestFail,
  fetchDimensions,
  updateDimension,
  reset,
  fetchHeatMapData,
  updateDates
};

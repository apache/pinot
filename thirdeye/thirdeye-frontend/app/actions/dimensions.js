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
  LOAD_TIMESERIES: type('[Dimensions] Load TimeSeries'),
  LOAD_HEATMAP: type('[Dimensions] Load HeatMap'),
  SET: type('[Dimension] Set Dimension'),
  RESET: type('[Dimensions] Reset Data'),
  REQUEST_FAIL: type('[Dimensions] Request Fail')
};

function resetData() {
  return {
    type: ActionTypes.RESET
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
      currentStart: analysisStart,
      currentEnd: analysisEnd,
      primaryMetricId,
      filters
    } = primaryMetric;

    const offset = COMPARE_MODE_MAPPING[compareMode] || 1;
    const baselineStart = moment(+analysisStart).subtract(offset, 'week').valueOf();
    const baselineEnd = moment(+analysisEnd).subtract(offset, 'week').valueOf();
    newDimension = newDimension || dimensions.selectedDimension;

    const url = `/timeseries/compare/${primaryMetricId}/${analysisStart}/${analysisEnd}/${baselineStart}/${baselineEnd}?dimension=${newDimension}&filters=${filters}&granularity=${granularity}`;
    const heatmapUrl = `/data/heatmap/${primaryMetricId}/${analysisStart}/${analysisEnd}/${baselineStart}/${baselineEnd}?filters=${filters}`;

    dispatch(setDimension(newDimension));
    return fetch(url)
      .then(res => res.json())
      .then(res => dispatch(loadTimeSeries(res)))
      .then(() => fetch(heatmapUrl))
      .then(res => res.json())
      .then(res => dispatch(loadHeatMap(res)));
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
  requestFail,
  fetchDimensions,
  updateDimension,
  reset
};

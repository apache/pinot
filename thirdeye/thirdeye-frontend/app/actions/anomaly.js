import { type } from './utils';
import fetch from 'fetch';

/**
 * Define the anomaly action types
 */
export const ActionTypes = {
  LOAD: type('[Anomaly] Load'),
  LOADING: type('[Anomaly] Loading'),
  REQUEST_FAIL: type('[Anomaly] Request Fail')
};

function loading() {
  return {
    type: ActionTypes.LOADING
  };
}

function loadAnomaly(response) {
  return {
    type: ActionTypes.LOAD,
    payload: response.anomalyDetailsList
  };
}

function requestFail() {
  return {
    type: ActionTypes.REQUEST_FAIL
  };
}

/**
 * Fetches the anomaly details for one anomaly
 *
 */
function fetchData(id) {
  return (dispatch) => {
    dispatch(loading());
    // TODO: save url in an API folder
    // need to have a new endpoint with just the anomaly details
    return fetch(`/anomalies/search/anomalyIds/1492498800000/1492585200000/1?anomalyIds=${id}&functionName=`)
      .then(res => res.json())
      .then(res => dispatch(loadAnomaly(res)))
      .catch(() => dispatch(requestFail()));
  };
}

export const Actions = {
  loading,
  loadAnomaly,
  requestFail,
  fetchData
};

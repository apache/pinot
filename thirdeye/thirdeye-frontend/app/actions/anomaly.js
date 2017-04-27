import { type } from './utils';

/**
 * Define the action types
 */
export const ActionTypes = {
  REQUEST_READ: type('[Anomaly] Request Read'),
  LOAD: type('[Anomaly] Load'),
  LOADING: type('[Anomaly] Loading'),
  REQUEST_FAIL: type('[Anomaly] Request Fail'),
};

function request(params) {
  return {
    type: ActionTypes.REQUEST_READ,
    payload: {
      params,
      source: 'search'
    }
  };
}

function loading() {
  return {
    type: ActionTypes.LOADING
  };
}

function loadAnomaly(response) {
  return {
    type: ActionTypes.LOAD,
    payload: response
  };
}

function requestFail() {
  return {
    type: ActionTypes.REQUEST_FAIL,
  };
}

export const Actions = {
  request,
  loading,
  loadAnomaly,
  requestFail
};

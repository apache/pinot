import Ember from 'ember';

/**
 * The Promise returned from fetch() won't reject on HTTP error status even if the response is an HTTP 404 or 500.
 * This helps us define a custom response handler.
 * @param {Object} response - the response object from a fetch call
 * @return {Object} either json-formatted payload or error object
 */
export function checkStatus(response) {
  if (response.status >= 200 && response.status < 300) {
    return JSON.parse(JSON.stringify(response));
  } else {
    const error = new Error(response.statusText);
    error.response = response;
    throw error;
  }
}

export default Ember.Helper.helper(checkStatus);

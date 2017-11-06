import Ember from 'ember';
import _ from 'lodash';

/**
 * The Promise returned from fetch() won't reject on HTTP error status even if the response is an HTTP 404 or 500.
 * This helps us define a custom response handler.
 * @param {Object} response - the response object from a fetch call
 * @param {String} mode - the request type: 'post', 'get'
 * @param {Boolean} recoverBlank - whether silent failure is allowed
 * @return {Object} either json-formatted payload or error object
 */
export function checkStatus(response, mode = 'get', recoverBlank = false) {
  if (response.status >= 200 && response.status < 300) {
    // Prevent parsing of null response
    if (response.status === 204) {
      return '';
    } else {
      return (mode === 'get') ? response.json() : JSON.parse(JSON.stringify(response));
    }
  } else {
    const error = new Error(response.statusText);
    error.response = response;
    if (recoverBlank) {
      return '';
    } else {
      throw error;
    }
  }
}

export function isIterable(obj) {
  if (obj == null || _.isString(obj)) {
    return false;
  }
  return typeof obj[Symbol.iterator] === 'function';
}

export function makeIterable(obj) {
  if (obj == null) {
    return [];
  }
  return isIterable(obj) ? [...obj] : [obj];
}

export function filterObject(obj, func) {
  const out = {};
  Object.keys(obj).filter(key => func(obj[key])).forEach(key => out[key] = obj[key]);
  return out;
}

export default Ember.Helper.helper({ checkStatus, isIterable, makeIterable, filterObject });

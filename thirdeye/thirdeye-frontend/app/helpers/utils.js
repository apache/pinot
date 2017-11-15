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

export function stripTail(urn) {
  const parts = urn.split(':');
  if (urn.startsWith('thirdeye:metric:')) {
    return _.slice(parts, 0, 3).join(':');
  }
  if (urn.startsWith('frontend:metric:')) {
    return _.slice(parts, 0, 4).join(':');
  }
  return urn;
}

export function toCurrentUrn(urn) {
  return metricUrnHelper('frontend:metric:current:', urn);
}

export function toBaselineUrn(urn) {
  return metricUrnHelper('frontend:metric:baseline:', urn);
}

export function toMetricUrn(urn) {
  return metricUrnHelper('thirdeye:metric:', urn);
}

function metricUrnHelper(prefix, urn) {
  const parts = urn.split(':');
  if (hasPrefix(urn, 'thirdeye:metric:')) {
    const tail = makeUrnTail(parts, 3);
    return `${prefix}${parts[2]}${tail}`;
  }
  if (hasPrefix(urn, 'frontend:metric:')) {
    const tail = makeUrnTail(parts, 4);
    return `${prefix}${parts[3]}${tail}`;
  }
  throw new Error(`Requires metric urn, but found ${urn}`);
}

function makeUrnTail(parts, baseLen) {
  return parts.length > baseLen ? ':' + _.slice(parts, baseLen).join(':') : '';
}

export function hasPrefix(urn, prefixes) {
  return !_.isEmpty(makeIterable(prefixes).filter(pre => urn.startsWith(pre)));
}

export function filterPrefix(urns, prefixes) {
  return makeIterable(urns).filter(urn => hasPrefix(urn, prefixes));
}

/**
 * finds the corresponding labelMapping field given a label in the filterBarConfig
 * This is only a placeholder since the filterBarConfig is not finalized
 */
export function findLabelMapping(label, config) {
  let labelMapping = '';
  config.some(filterBlock => filterBlock.inputs.some(input => {
    if (input.label === label) {
      labelMapping = input.labelMapping;
    }
  }));
  return labelMapping;
}

export default Ember.Helper.helper({ checkStatus, isIterable, makeIterable, filterObject, toCurrentUrn, toBaselineUrn, toMetricUrn, stripTail, hasPrefix, filterPrefix, findLabelMapping });

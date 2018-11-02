import d3 from 'd3';
import { isNone } from '@ember/utils';
import moment from 'moment';
import { splitFilterFragment, toFilterMap } from 'thirdeye-frontend/utils/rca-utils';

/**
 * The Promise returned from fetch() won't reject on HTTP error status even if the response is an HTTP 404 or 500.
 * This helps us define a custom response handler.
 * @param {Object} response - the response object from a fetch call
 * @param {String} mode - the request type: 'post', 'get'
 * @param {Boolean} recoverBlank - whether silent failure is allowed
 * @return {Object} either json-formatted payload or error object
 */
export function checkStatus(response, mode = 'get', recoverBlank = false) {
  if (response.status === 401) {
    // We want to throw a 401 error up the error substate(s) to handle it
    throw new Error('401');
  } else if (response.status >= 200 && response.status < 300) {
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

/**
 * Helper for testing a number (or reference) for a valid display value
 * @param {float} f float number
 */
function isValidForDisplay(f) {
  return !(isNone(f) || typeof f !== 'number' || Number.isNaN(f));
}

/**
 * Formatter for human-readable floating point numbers
 * @param {float} f float number
 * @return {string} human-readable number string
 *
 * @example
 * 5.678    =>   5.68
 * 2.9e11   => 290.00B
 * 1.234e-4 =>   0.12m
 */
export function humanizeFloat(f) {
  if (!isValidForDisplay(f)) { return '-'; }
  const formattedNum = d3.format('.3s')(f);
  // Catch/replace meaningless micro value
  const isMicroNum = (new RegExp(/0\.0+y$/)).test(formattedNum);
  return isMicroNum ? 0 : formattedNum;
}

/**
 * Formatter for the human-readable change values displayed in percent with 1 decimal. Truncates extreme values > 10x.
 * @param {float} f float number
 * @return {string} human-readable change string
 *
 * @example
 * 0.5177  => +51.8%
 * -0.0001 => -0.0%
 * 10.001  => +1000%+
 * -10.01  => -1000%+
 */
export function humanizeChange(f) {
  if (!isValidForDisplay(f)) { return '-'; }
  if (f > 10) {
    return '+1000%+';
  } else if (f < -10) {
    return '-1000%+';
  }
  return `${f > 0 ? '+' : ''}${Math.round(f * 100).toFixed(1)}%`;
}

/**
 * Formatter for human-readable entity scores with 2 decimals
 * @param {float} f float number
 * @return {string} human-readable score string
 */
export function humanizeScore(f) {
  if (!isValidForDisplay(f)) { return '-'; }
  return f.toFixed(2);
}

/**
 * Helps with shorthand for repetitive date generation
 */
export function buildDateEod(unit, type) {
  return moment().subtract(unit, type).endOf('day').utc();
}

/**
 * Parses stringified object from payload
 * @param {String} filters
 * @returns {Object}
 */
export function parseProps(filters) {
  filters = filters || '';

  return filters.split(';')
    .filter(prop => prop)
    .map(prop => prop.split('='))
    .reduce(function (aggr, prop) {
      const [ propName, value ] = prop;
      aggr[propName] = value;
      return aggr;
    }, {});
}

/**
 * Takes a raw filter string and processes it via 'splitFilterFragment' into a API-compatible JSON-formatted string.
 * "filter1=value;filter2=value"
 * @method makeFilterString
 * @param {Array} filtersRaw - single current record
 * @private
 */
export function makeFilterString(filtersRaw) {
  try {
    return JSON.stringify(toFilterMap(filtersRaw.split(';').map(splitFilterFragment)));
  } catch (ignore) {
    return '';
  }
}

/**
 * Preps post object and stringifies post data
 * @param {Object} data to post
 * @returns {Object}
 */
export function postProps(postData) {
  return {
    method: 'post',
    body: JSON.stringify(postData),
    headers: { 'content-type': 'Application/Json' }
  };
}

/**
 * Format conversion helper
 * @param {String} dateStr - date to convert
 */
export function toIso(dateStr) {
  return moment(Number(dateStr)).toISOString();
}

export default {
  checkStatus,
  humanizeFloat,
  humanizeChange,
  humanizeScore,
  makeFilterString,
  parseProps,
  postProps,
  toIso
};

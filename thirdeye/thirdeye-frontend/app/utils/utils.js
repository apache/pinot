import { isNone } from '@ember/utils';
import moment from 'moment';

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

  const log10 = Math.log10(Math.abs(f));

  let suffix = '', shift = 0;
  if (log10 >= 15) {
    return '+inf';
  } else if (log10 >= 12) { // 1,000,000,000,000
    suffix = 'T';
    shift = -12;
  } else if (log10 >= 9) { // 1,000,000,000
    suffix = 'B';
    shift = -9;
  } else if (log10 >= 6) { // 1,000,000
    suffix = 'M';
    shift = -6;
  } else if (log10 >= 3) { // 1,000
    suffix = 'K';
    shift = -3;
  } else if (log10 >= -2) { // 0.01
    suffix = '';
    shift = 0;
  } else if (log10 >= -5) { // 0.000,01
    suffix = 'm';
    shift = 3;
  } else {
    return '-inf';
  }

  return `${(f * Math.pow(10, shift)).toFixed(2)}${suffix}`;
}

/**
 * Formatter for the human-readable change values in percent with 1 decimal
 * @param {float} f float number
 * @return {string} human-readable change string
 */
export function humanizeChange(f) {
  if (!isValidForDisplay(f)) { return '-'; }
  if (Math.abs(f) < 10.0) {
    return `${f > 0 ? '+' : ''}${Math.round(f * 100).toFixed(1)}%`;
  } else {
    return `${f > 0 ? '+' : '-'}1000%+`;
  }
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
  parseProps,
  postProps,
  toIso
};

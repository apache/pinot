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
 * Formatter for the human-readable floating point numbers numbers
 */
export function humanizeFloat(f) {
  if (isNone(f) || Number.isNaN(f)) { return '-'; }
  const fixed = Math.max(3 - Math.max(Math.floor(Math.log10(f)) + 1, 0), 0);
  return f.toFixed(fixed);
}

/**
 * Formatter for the human-readable change values in percent with 1 decimal
 */
export function humanizeChange(f) {
  if (isNone(f) || Number.isNaN(f)) { return '-'; }
  return `${f > 0 ? '+' : ''}${(Math.round(f * 1000) / 10.0).toFixed(1)}%`;
}

/**
 * Formatter for human-readable entity scores with 2 decimals
 */
export function humanizeScore(f) {
  if (isNone(f) || Number.isNaN(f)) { return '-'; }
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

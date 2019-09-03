import d3 from 'd3';
import { isNone } from '@ember/utils';
import moment from 'moment';
import { splitFilterFragment, toFilterMap, makeTime } from 'thirdeye-frontend/utils/rca-utils';

/**
 * The Promise returned from fetch() won't reject on HTTP error status even if the response is an HTTP 404 or 500.
 * This helps us define a custom response handler.
 * @param {Object} response - the response object from a fetch call
 * @param {String} mode - the request type: 'post', 'get'
 * @param {Boolean} recoverBlank - whether silent failure is allowed
 * @param {Boolean} isYamlPreview - handle response differently for post of yaml preview
 * @return {Object} either json-formatted payload or error object
 */
export function checkStatus(response, mode = 'get', recoverBlank = false, isYamlPreview = false) {
  if (response.status === 401) {
    // We want to throw a 401 error up the error substate(s) to handle it
    throw new Error('401');
  } else if (response.status >= 200 && response.status < 300) {
    // Prevent parsing of null response
    if (response.status === 204) {
      return '';
    } else {
      return (mode === 'get' || isYamlPreview) ? response.json() : JSON.parse(JSON.stringify(response));
    }
  } else {
    if (isYamlPreview) {
      return response.json()
        .then(data => {
          const error = new Error(data);
          error.body= data;
          throw error;
        });
    }
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
  return makeTime().subtract(unit, type).startOf('day');
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
 * Preps get object
 * @returns {Object}
 */
export function getProps() {
  return {
    method: 'get',
    headers: { 'content-type': 'Application/Json' }
  };
}

/**
 * Preps post object for Yaml payload
 * @param {string} text to post
 * @returns {Object}
 */
export function postYamlProps(postData) {
  return {
    method: 'post',
    body: postData,
    headers: { 'content-type': 'text/plain' }
  };
}

/**
 * Format conversion helper
 * @param {String} dateStr - date to convert
 */
export function toIso(dateStr) {
  return moment(Number(dateStr)).toISOString();
}

/**
 * Replace all Infinity and NaN with null in array of numbers
 * @param {Array} timeSeries - time series to modify
 */
export function stripNonFiniteValues(timeSeries) {
  return timeSeries.map(value => {
    return (isFinite(value) ? value : null);
  });
}

/**
 * The yaml filters formatter. Convert filters in the yaml file in to a legacy filters string
 * For example, filters = {
 *   "country": ["us", "cn"],
 *   "browser": ["chrome"]
 * }
 * will be convert into "country=us;country=cn;browser=chrome"
 *
 * @method _formatYamlFilter
 * @param {Map} filters multimap of filters
 * @return {String} - formatted filters string
 */
export function formatYamlFilter(filters) {
  if (filters){
    const filterStrings = [];
    Object.keys(filters).forEach(
      function(filterKey) {
        const filter = filters[filterKey];
        if (filter && Array.isArray(filter)) {
          filter.forEach(
            function (filterValue) {
              filterStrings.push(filterKey + '=' + filterValue);
            }
          );
        } else {
          filterStrings.push(filterKey + '=' + filter);
        }
      }
    );
    return filterStrings.join(';');
  }
  return '';
}

export default {
  checkStatus,
  humanizeFloat,
  humanizeChange,
  humanizeScore,
  makeFilterString,
  parseProps,
  postProps,
  toIso,
  stripNonFiniteValues,
  postYamlProps,
  formatYamlFilter,
  getProps
};

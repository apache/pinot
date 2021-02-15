import d3 from 'd3';
import { isNone } from '@ember/utils';
import moment from 'moment';
import { splitFilterFragment, toFilterMap, makeTime } from 'thirdeye-frontend/utils/rca-utils';
import _ from 'lodash';

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
    if (mode === 'delete' || response.status === 204) {
      return '';
    } else {
      return mode === 'get' || isYamlPreview ? response.json() : JSON.parse(JSON.stringify(response));
    }
  } else {
    if (isYamlPreview) {
      return response.json().then((data) => {
        const error = new Error(data);
        error.body = data;
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
  if (!isValidForDisplay(f)) {
    return '-';
  }
  const formattedNum = d3.format('.3s')(f);
  // Catch/replace meaningless micro value
  const isMicroNum = new RegExp(/0\.0+y$/).test(formattedNum);
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
  if (!isValidForDisplay(f)) {
    return '-';
  }
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
  if (!isValidForDisplay(f)) {
    return '-';
  }
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

  return filters
    .split(';')
    .filter((prop) => prop)
    .map((prop) => prop.split('='))
    .reduce(function (aggr, prop) {
      const [propName, value] = prop;
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
 * Preps put object
 * @returns {Object}
 */
export function putProps() {
  return {
    method: 'put',
    body: '',
    headers: { 'content-type': 'text/plain' }
  };
}

/**
 * Preps delete object
 * @returns {Object}
 */
export function deleteProps() {
  return {
    method: 'delete',
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

/**
 * Replace all Infinity and NaN with value from second time series
 * @param {Array} series1 - time series to modify
 * @param {Array} series2 - time series to get replacement values from
 */
export function replaceNonFiniteWithCurrent(series1, series2) {
  if (_.isEmpty(series2)) {
    return stripNonFiniteValues(series1);
  }
  for (let i = 0; i < series1.length; i++) {
    if (!isFinite(series1[i]) || (!series1[i] && series1[i] !== 0)) {
      let newValue = null;
      if (i < series2.length) {
        newValue = isFinite(series2[i]) ? series2[i] : null;
      }
      series1[i] = newValue;
    }
  }
  return series1;
}

/**
 * Check if all values in array are null
 * @param {Array} toCheck - array to check for all nulls
 */
export function isAllNull(toCheck) {
  return _.isEmpty(toCheck.filter((value) => value !== null));
}

/**
 * Builds upper and lower bounds, as relevant
 * @param {Object} series - object that holds series that will be passed to time series chart component
 * @param {Object} baseline - baseline object taken from json response
 * @param {Object} timeseries - time series object taken from json response
 * @param {Boolean} useCurrent - whether to use current or value, decided by caller
 */
export function buildBounds(series, baseline, timeseries, useCurrent) {
  if (baseline) {
    const upperExists = !_.isEmpty(baseline.upper_bound) && !isAllNull(baseline.upper_bound);
    const lowerExists = !_.isEmpty(baseline.lower_bound) && !isAllNull(baseline.lower_bound);
    if (upperExists && lowerExists) {
      series['Upper and lower bound'] = {
        timestamps: baseline.timestamp,
        values: replaceNonFiniteWithCurrent(baseline.upper_bound, useCurrent ? timeseries.current : timeseries.value),
        type: 'line',
        color: 'screenshot-bounds'
      };
      series['lowerBound'] = {
        timestamps: baseline.timestamp,
        values: replaceNonFiniteWithCurrent(baseline.lower_bound, useCurrent ? timeseries.current : timeseries.value),
        type: 'line',
        color: 'screenshot-bounds'
      };
    } else if (upperExists) {
      series['Upper Bound'] = {
        timestamps: baseline.timestamp,
        values: replaceNonFiniteWithCurrent(baseline.upper_bound, useCurrent ? timeseries.current : timeseries.value),
        type: 'line',
        color: 'screenshot-bounds'
      };
    } else if (lowerExists) {
      series['Lower Bound'] = {
        timestamps: baseline.timestamp,
        values: replaceNonFiniteWithCurrent(baseline.lower_bound, useCurrent ? timeseries.current : timeseries.value),
        type: 'line',
        color: 'screenshot-bounds'
      };
    }
  }
}

/**
 * Replace all Infinity and NaN with null in array of numbers
 * @param {Array} timeSeries - time series to modify
 */
export function stripNonFiniteValues(timeSeries) {
  return timeSeries.map((value) => {
    return isFinite(value) ? value : null;
  });
}

/*
 * Detect if the input is in Object form
 *
 * @param {Any} input
 *   The input to test.
 *
 * @returns {Boolean}
 *   True if input is an object, false otherwise.
 */
export function isObject(input) {
  return input instanceof Object && input.constructor === Object;
}

/*
 * Search the input for the existence of the search term
 *   -Supports searching on following input types - string, integer, float, boolean, object, array
 *
 * @param {Any} input
 *   The input to test.
 * @param {String} filterStr
 *   The search term
 *
 * @returns {Boolean}
 *   True if the match is found, false otherwise.
 */
export function checkForMatch(input, filterStr) {
  if (filterStr === '') {
    return true;
  }

  if (typeof input === 'string') {
    return input.includes(filterStr);
  } else if (typeof input === 'number' || typeof input === 'boolean') {
    return input.toString().includes(filterStr);
  } else if (isObject(input)) {
    for (const prop in input) {
      if (
        {}.propertyIsEnumerable.call(input, prop) &&
        (checkForMatch(prop, filterStr) || checkForMatch(input[prop], filterStr))
      ) {
        return true;
      }
    }

    return false;
  } else if (Array.isArray(input)) {
    for (const entry of input) {
      if (checkForMatch(entry, filterStr)) {
        return true;
      }
    }

    return false;
  }

  return false;
}

export default {
  checkStatus,
  humanizeFloat,
  humanizeChange,
  humanizeScore,
  makeFilterString,
  parseProps,
  postProps,
  deleteProps,
  putProps,
  toIso,
  replaceNonFiniteWithCurrent,
  stripNonFiniteValues,
  getProps,
  isAllNull,
  buildBounds,
  isObject,
  checkForMatch
};

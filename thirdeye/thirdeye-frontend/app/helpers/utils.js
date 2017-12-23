import Ember from 'ember';
import moment from 'moment';
import _ from 'lodash';

// TODO load from config
// colors for metrics
export const metricColors = [
  'blue',
  'green',
  'red',
  'purple',
  'orange',
  'teal',
  'pink',
  'grey'
];

// TODO load from config
// colors mapping for charts
export const colorMapping = {
  blue: '#0091CA', // 5/10
  orange: '#E55800',
  teal: '#0E95A0',
  purple: '#827BE9',
  red: '#FF2C33',
  green: '#469A1F',
  pink: '#FF1B90',
  grey: '#878787',
  'light-blue': '#98D8F4', // 2/10
  'light-green': '#B5D99F',
  'light-red': '#FFBCBA',
  'light-purple': '#CACAFC',
  'light-orange': '#F8C19E',
  'light-teal': '#98DADE',
  'light-pink': '#FFB9E2',
  'light-grey': '#CFCFCF'
};

// TODO load from config
// colors for events
export const eventColorMapping = {
  holiday: 'green',
  informed: 'red',
  lix: 'purple',
  gcn: 'orange',
  anomaly: 'teal'
};

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
 * Pluralizes and formats the anomaly range duration string
 * @param {Number} time
 * @param {String} unit
 * @returns {String}
 */
export function pluralizeTime(time, unit) {
  const unitStr = time > 1 ? unit + 's' : unit;
  return time ? time + ' ' + unitStr : '';
}

/**
 * Formatter for the human-readable floating point numbers numbers
 */
export function humanizeFloat(f) {
  if (f == null || Number.isNaN(f)) { return '-'; }
  const fixed = Math.max(3 - Math.max(Math.floor(Math.log10(f)) + 1, 0), 0);
  return f.toFixed(fixed);
};

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

export function extractTail(urn) {
  const parts = urn.split(':');
  if (urn.startsWith('thirdeye:metric:')) {
    return _.slice(parts, 3);
  }
  if (urn.startsWith('frontend:metric:')) {
    return _.slice(parts, 4);
  }
  return [];
}

export function appendTail(urn, tail) {
  if (_.isEmpty(tail)) {
    return urn;
  }

  const existingTail = extractTail(urn);
  const tailString = [...new Set([...makeIterable(tail), ...existingTail])].sort().join(':');
  const appendString = tailString ? `:${tailString}` : '';
  return `${stripTail(urn)}${appendString}`;
}

export function appendFilters(urn, filters) {
  const tail = filters.map(t => `${t[0]}=${t[1]}`);
  return appendTail(urn, tail);
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

export function toMetricLabel(urn, entities) {
  let metricName = stripTail(urn);
  if (entities && entities[stripTail(urn)]) {
    metricName = entities[stripTail(urn)].label.split("::")[1];
  }

  const filters = toFilters(urn).map(t => t[1]);
  const filterString = filters.length ? ` (${filters.join(', ')})` : '';

  return `${metricName}${filterString}`;
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

export function toBaselineRange(anomalyRange, compareMode) {
  const offset = {
    WoW: 1,
    Wo2W: 2,
    Wo3W: 3,
    Wo4W: 4
  }[compareMode];

  const start = moment(anomalyRange[0]).subtract(offset, 'weeks').valueOf();
  const end = moment(anomalyRange[1]).subtract(offset, 'weeks').valueOf();

  return [start, end];
}

export function toFilters(urns) {
  const flatten = (agg, l) => agg.concat(l);

  const dimensionFilters = filterPrefix(urns, 'thirdeye:dimension:').map(urn => _.slice(urn.split(':'), 2, 4));
  const metricFilters = filterPrefix(urns, 'thirdeye:metric:').map(extractTail).map(enc => enc.map(tup => tup.split('='))).reduce(flatten, []);
  const frontendMetricFilters = filterPrefix(urns, 'frontend:metric:').map(extractTail).map(enc => enc.map(tup => tup.split('='))).reduce(flatten, []);
  return [...dimensionFilters, ...metricFilters, ...frontendMetricFilters].sort();
}

export function fromFilterMap(filterMap) {
  const filters = [];
  Object.keys(filterMap).forEach(key => {
    [...filterMap[key]].forEach(value => {
      filters.push([key, value]);
    })
  });
  return filters;
}

export function toFilterMap(filters) {
  const filterMap = {};
  filters.forEach(t => {
    const [dimName, dimValue] = t;
    if (!filterMap[dimName]) {
      filterMap[dimName] = new Set();
    }
    filterMap[dimName].add(dimValue);
  });

  // Set to list
  Object.keys(filterMap).forEach(dimName => filterMap[dimName] = [...filterMap[dimName]]);

  return filterMap;
}

export function toColor(urn) {
  // TODO move to controller, requires color loading from backend
  if (urn.startsWith('thirdeye:event:')) {
    return eventColorMapping[urn.split(':')[2]];
  }
  if (urn.startsWith('thirdeye:metric:')) {
    return metricColors[urn.split(':')[2] % metricColors.length];
  }
  return 'none';
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

export default Ember.Helper.helper({
  checkStatus,
  pluralizeTime,
  isIterable,
  makeIterable,
  filterObject,
  toCurrentUrn,
  toBaselineUrn,
  toMetricUrn,
  stripTail,
  extractTail,
  appendTail,
  hasPrefix,
  filterPrefix,
  toBaselineRange,
  toFilters,
  toFilterMap,
  findLabelMapping,
  toMetricLabel,
  toColor,
  humanizeFloat,
  fromFilterMap,
  appendFilters,
  metricColors,
  colorMapping,
  eventColorMapping,
  buildDateEod,
  parseProps,
  postProps
});

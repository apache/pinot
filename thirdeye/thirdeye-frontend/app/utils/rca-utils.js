import moment from 'moment';
import _ from 'lodash';
import config from 'thirdeye-frontend/config/environment';


const ROOTCAUSE_ANALYSIS_DURATION_MAX = 1209600000; // 14 days (in millis)
const ROOTCAUSE_ANOMALY_DURATION_MAX = 604800000; // 7 days (in millis)

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
  grey: '#B4B4B4',
  'light-blue': '#98D8F4', // 2/10
  'light-green': '#B5D99F',
  'light-red': '#FFBCBA',
  'light-purple': '#CACAFC',
  'light-orange': '#F8C19E',
  'light-teal': '#98DADE',
  'light-pink': '#FFB9E2',
  'light-grey': '#CFCFCF',
  'confidence-bounds-blue' : '#dcf8f3',
  'screenshot-current' : '#1B1B1E',
  'screenshot-predicted' : '#FF9505',
  'screenshot-anomaly' : '#EEF2F5',
  'screenshot-bounds' : '#1CAFED'
};

// TODO load from config
// colors for events
export const eventColorMapping = {
  holiday: 'green',
  informed: 'red',
  lix: 'purple',
  gcn: 'orange',
  anomaly: 'teal',
  cm: 'grey',
  custom: 'pink'
};

export const dateFormatFull = 'ddd, MMM D YYYY, h:mm a z';

/**
 * Returns a timestamp pinned to the default RCA time zone
 */
export function makeTime(t, args) {
  if (typeof t === 'undefined') { t = moment().valueOf(); }
  return moment(t, args).tz(config.timeZone);
}

/**
 * Parses the input as float and returns it, unless it is NaN where it returns Number.NEGATIVE_INFINITY instead
 */
export function makeSortable(f) {
  const n = parseFloat(f);
  if (Number.isNaN(n)) {
    return Number.NEGATIVE_INFINITY;
  }
  return n;
}

/**
 * Returns true for collection-like objects with an iterator function, but treats strings as atomic item. Also null-safe.
 */
export function isIterable(obj) {
  if (typeof obj === 'undefined' || obj === null || _.isString(obj)) {
    return false;
  }
  return typeof obj[Symbol.iterator] === 'function';
}

/**
 * Turns both atomic and collection-like objects into an iterable array. Null safe.
 *
 * @see isIterable(obj)
 */
export function makeIterable(obj) {
  if (obj === null || obj === undefined) {
    return [];
  }
  return isIterable(obj) ? [...obj] : [obj];
}

/**
 * Returns a copy of {obj} whose immediate properties have been filtered by {func}, similar Array.filter().
 *
 * @param {Object} obj object to filter on
 * @param {Function} func filter function, returning true or false
 */
export function filterObject(obj, func) {
  const out = {};
  Object.keys(obj).filter(key => func(obj[key])).forEach(key => out[key] = obj[key]);
  return out;
}


/**
 * Returns the base entity URN by removing any optional long tail.
 * Example: 'thirdeye:metric:123:country=IT' returns 'thirdeye:metric:123'
 *
 * @param {string} urn entity urn
 * @returns {string} base urn without tail
 */
export function stripTail(urn) {
  const parts = urn.split(':');
  if (urn.startsWith('thirdeye:metric:')) {
    return _.slice(parts, 0, 3).join(':');
  }
  if (urn.startsWith('frontend:metric:')) {
    return _.slice(parts, 0, 4).join(':');
  }
  if (urn.startsWith('frontend:anomalyfunction:')) {
    return _.slice(parts, 0, 3).join(':');
  }
  if (urn.startsWith('thirdeye:event:anomaly:')) {
    return _.slice(parts, 0, 4).join(':');
  }
  if (urn.startsWith('thirdeye:dimensions:')) {
    return _.slice(parts, 0, 2).join(':');
  }
  if (urn.startsWith('thirdeye:callgraph:')) {
    return _.slice(parts, 0, 2).join(':');
  }
  return urn;
}

/**
 * Returns the tail fragments of a long-form entity urn as Array. Returns an empty array if no tail exists.
 * Example: 'thirdeye:metric:123:country=IT:page=start' returns ['country=IT', 'page=start']
 *
 * @param {string} urn entity urn
 * @returns {Array} tail fragments
 */
export function extractTail(urn) {
  const parts = urn.split(':');
  if (urn.startsWith('thirdeye:metric:')) {
    return _.slice(parts, 3).filter(p => !_.isEmpty(p));
  }
  if (urn.startsWith('frontend:metric:')) {
    return _.slice(parts, 4).filter(p => !_.isEmpty(p));
  }
  if (urn.startsWith('frontend:anomalyfunction:')) {
    return _.slice(parts, 3).filter(p => !_.isEmpty(p));
  }
  if (urn.startsWith('thirdeye:event:anomaly:')) {
    return _.slice(parts, 4).filter(p => !_.isEmpty(p));
  }
  if (urn.startsWith('thirdeye:dimensions:')) {
    return _.slice(parts, 2).filter(p => !_.isEmpty(p));
  }
  if (urn.startsWith('thirdeye:callgraph:')) {
    return _.slice(parts, 2).filter(p => !_.isEmpty(p));
  }
  return [];
}

/**
 * Appends tail fragments to an existing urn. Merges with existing tail fragments and applies them in sorted order.
 *
 * @param {String} urn entity urn
 * @param {Array} tail tail fragments array
 * @returns {String} merged entity urn
 */
export function appendTail(urn, tail) {
  if (_.isEmpty(tail)) {
    return urn;
  }

  const existingTail = extractTail(urn);
  const tailString = [...new Set([...makeIterable(tail), ...existingTail])].sort().join(':');
  const appendString = tailString ? `:${tailString}` : '';
  return `${stripTail(urn)}${appendString}`;
}

/**
 * Appends filter fragments to a metric urn. Works similar to appendTail(), but converts filter tuples into Strings first.
 *
 * @see appendTail(urn, tail)
 *
 * @param {string} urn entity urn
 * @param {Array} filters array of strings ["key", "=", "value"]
 * @returns {string} merged metric urn
 */
export function appendFilters(urn, filters) {
  const tail = filters.map(t => {
    // protects against special characters being in value string
    const encodedValue = encodeURIComponent(t[2]);
    return encodeURIComponent(`${t[0]}${t[1]}${encodedValue}`);
  });
  return appendTail(urn, tail);
}

/**
 * Converts any metric urn to its frontend metric-reference equivalent with a 'current' offset
 * Example: 'thirdeye:metric:123:country=IT' returns 'frontend:metric:current:123:country=IT'
 *
 * @param {string} urn metric urn
 * @returns {string} frontend metric-reference urn with offset 'current'
 */
export function toCurrentUrn(urn) {
  return metricUrnHelper('frontend:metric:current:', urn);
}

/**
 * Converts any metric urn to its frontend metric-reference equivalent with a 'baseline' offset
 * Example: 'thirdeye:metric:123:country=IT' returns 'frontend:metric:baseline:123:country=IT'
 *
 * @param {string} urn metric urn
 * @returns {string} frontend metric-reference urn with offset 'baseline'
 */
export function toBaselineUrn(urn) {
  return metricUrnHelper('frontend:metric:baseline:', urn);
}

/**
 * Converts any metric urn to its dimensions equivalent
 * Example: 'thirdeye:metric:123:country=IT' returns 'thirdeye:dimensions:country=IT'
 *
 * @param {string} urn metric urn
 * @returns {string} dimensions urn
 */
export function toDimensionsUrn(urn) {
  return appendTail('thirdeye:dimensions:', extractTail(urn));
}

/**
 * Converts any metric urn to its frontend metric-reference equivalent, with an user-specified offset.
 *
 * @param {string} urn metric urn
 * @param {string} offset metric reference offset ('current', 'baseline', 'wo1w', 'wo2w', 'wo3w', 'wo4w)
 * @returns {string} frontend metric-reference urn with given offset
 */
export function toOffsetUrn(urn, offset) {
  return metricUrnHelper(`frontend:metric:${offset}:`, urn);
}

/**
 * Converts any metric urn to its frontend metric-reference equivalent, with an absolute time offset.
 * (I.e. the resulting metric urn does not use 'baseline', but rather the compare mode specified in the context)
 *
 * @param {string} urn metric urn
 * @param {string} contextCompareMode compare mode offset ('wo1w', 'wo2w', 'wo3w', 'wo4w')
 * @returns {string} frontend metric-reference urn with given offset
 */
export function toAbsoluteUrn(urn, contextCompareMode) {
  if (urn.startsWith('thirdeye:metric:')) {
    urn = toCurrentUrn(urn);
  }

  if (!urn.startsWith('frontend:metric:')) {
    return urn;
  }

  let offset = urn.split(':')[2].toLowerCase();
  if (offset === 'baseline') {
    offset = contextCompareMode.toLowerCase();
  }

  if (offset === 'wow') {
    offset = 'wo1w';
  }

  return metricUrnHelper(`frontend:metric:${offset}:`, urn);
}

/**
 * Converts any metric urn to its entity equivalent
 * Example: 'frontend:metric:wo2w:123:country=IT' returns 'thirdeye:metric:123:country=IT'
 *
 * @param {string} urn metric urn
 */
export function toMetricUrn(urn) {
  return metricUrnHelper('thirdeye:metric:', urn);
}

/**
 * Converts any percent string to number
 * Example: '34%' returns 34
 *
 * @param {string} width - percent string
 */
export function toWidthNumber(width) {
  return Number(width.replace('%', ''));
}

/**
 * Returns a human-readable label for a metric urn, optionally using information from the entities cache.
 *
 * @param {string} urn metric urn
 * @param {Object} entities entities cache
 * @returns {string} human-readable metric label
 */
export function toMetricLabel(urn, entities) {
  let metricName;
  try {
    metricName = entities[urn].label.split("::")[1].split("_").join(' ');
  }
  catch (err) {
    metricName = urn;
  }

  const filters = toFilters(urn).map(t => filter2value(t));

  // TODO support range filters
  const inclusionFilters = filters.filter(f => !f.startsWith('!')).sort();
  const exclusionFilters = filters.filter(f => f.startsWith('!')).map(f => f.substring(1)).sort();

  const inclusionFiltersString = _.isEmpty(inclusionFilters) ? '' : ` (${inclusionFilters.join(', ')})`;
  const exclusionFiltersString = _.isEmpty(exclusionFilters) ? '' : ` (Excludes ${exclusionFilters.join(', ')})`;

  return `${metricName}${inclusionFiltersString}${exclusionFiltersString}`;
}

/**
 * Returns a human-readable dataset name for a metric urn, using information from the entities cache.
 *
 * @param {string} urn metric urn
 * @param {object} entities entities cache
 */
export function toMetricDataset(urn, entities) {
  try {
    return entities[urn].attributes.dataset[0];
  } catch (err) {
    return '-';
  }
}

/**
 * Returns a human-readable label for an event urn
 *
 * @param {string} urn event urn
 * @param {Object} entities entities cache
 * @returns {string} human-readable event label
 */
export function toEventLabel(urn, entities) {
  let label;
  try {
    label = entities[urn].label;
  }
  catch (err) {
    label = urn;
  }

  if (urn.includes('anomaly')) {
    const [, id] = urn.split(':anomaly:');
    label = `Anomaly #${id} ${label}`;
  }

  return label;
}

/**
 * Helper to replace metric urn prefixes of entity urns and reference urns.
 *
 * @param {string} prefix
 * @param {string} urn
 * @returns {string} urn with given prefix
 */
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
  if (hasPrefix(urn, 'frontend:anomalyfunction:')) {
    const tail = makeUrnTail(parts, 3);
    return `${prefix}${parts[2]}${tail}`;
  }
  if (hasPrefix(urn, 'thirdeye:event:anomaly:')) {
    const tail = makeUrnTail(parts, 4);
    return `${prefix}${parts[2]}${tail}`;
  }
  throw new Error(`Requires supported urn, but found ${urn}`);
}

/**
 * Helper to append an optional tail to a urn
 *
 * @param {Array} parts urn fragments
 * @param {int} baseLen length of the base urn, in fragments
 * @returns {string} urn with optional tail
 */
function makeUrnTail(parts, baseLen) {
  return parts.length > baseLen ? ':' + _.slice(parts, baseLen).join(':') : '';
}

/**
 * Returns true if the given urn matches AT LEAST one of the given prefixes, otherwise false.
 * NOTE: specify prefixes with ':' at the end to avoid matching partial fragments
 *
 * @param {string} urn entity urn
 * @param {Array} prefixes array of candidate prefixes
 * @returns {boolean}
 */
export function hasPrefix(urn, prefixes) {
  return urn && !_.isEmpty(makeIterable(prefixes).filter(pre => urn.startsWith(pre)));
}

/**
 * Filters an array of urns for a given set of candidate prefixes using hasPrefix(urn, prefixes).
 *
 * @see hasPrefix(urn, prefixes)
 *
 * @param {Array} urns array of entity urns
 * @param {Array} prefixes array of candidate prefixes
 * @returns {boolean}
 */
export function filterPrefix(urns, prefixes) {
  return makeIterable(urns).filter(urn => hasPrefix(urn, prefixes));
}

/**
 * Converts a time range tuple to another time range with a given offset (in Pacific time zone)
 *
 * @param {Array} range time range tuple [start, end]
 * @param {string} offset time offset ('current', 'baseline', 'wo1w', 'wo2w', 'wo3w', 'wo4w)
 * @returns {Array} offset time range tuple
 */
export function toBaselineRange(range, offset) {
  const timeOffset = {
    current: [0, 'weeks'],
    predicted: [0, 'weeks'], // no backend support
    none: [0, 'weeks'], // no backend support

    wow: [1, 'weeks'],
    wo1w: [1, 'weeks'],
    wo2w: [2, 'weeks'],
    wo3w: [3, 'weeks'],
    wo4w: [4, 'weeks'],
    mean4w: [1, 'weeks'], // no backend support
    median4w: [1, 'weeks'], // no backend support
    min4w: [1, 'weeks'], // no backend support
    max4w: [1, 'weeks'], // no backend support

    ho1h: [1, 'hours'],
    ho2h: [2, 'hours'],
    ho3h: [3, 'hours'],
    ho6h: [6, 'hours'],
    median6h: [1, 'hours'], // no backend support
    mean6h: [1, 'hours'], // no backend support
    min6h: [1, 'hours'], // no backend support
    max6h: [1, 'hours'], // no backend support

    do1d: [1, 'days'],
    do2d: [2, 'days'],
    do3d: [3, 'days'],
    do4d: [4, 'days'],
    median4d: [1, 'days'], // no backend support
    mean4d: [1, 'days'], // no backend support
    min4d: [1, 'days'], // no backend support
    max4d: [1, 'days'], // no backend support

    mo1m: [1, 'months'],
    mo2m: [2, 'months'],
    mo3m: [3, 'months'],
    mo6m: [6, 'months'],
    median6m: [1, 'months'], // no backend support
    mean6m: [1, 'months'], // no backend support
    min6m: [1, 'months'], // no backend support
    max6m: [1, 'months'] // no backend support

  }[offset.toLowerCase()];

  if (!timeOffset || timeOffset[0] === 0) {
    return range;
  }

  const start = makeTime(range[0]).subtract(timeOffset[0], timeOffset[1]).valueOf();
  const end = start + (range[1] - range[0]);

  return [start, end];
}

/**
 * Replace frontend metric-reference 'baseline' offset with absolute offset
 *
 * @param {string} urn frontend metric-reference urn
 * @param {Array} currentRange current time range tuple [start, end]
 * @param {string} baselineCompareMode absolute offset for baseline ('wo1w', 'wo2w', 'wo3w', 'wo4w')
 * @returns {string} frontend metric-reference urn with absolute offset
 */
export function toAbsoluteRange(urn, currentRange, baselineCompareMode) {
  if (!urn.startsWith('frontend:metric:')) {
    return currentRange;
  }

  let compareMode = urn.split(':')[2];
  if (compareMode === 'baseline') {
    compareMode = baselineCompareMode;
  }

  return toBaselineRange(currentRange, compareMode);
}

/**
 * Extract filter tuples from urns. Supports 'thirdeye:dimension:', 'thirdeye:metric:', 'frontend:metric:' prefixes.
 *
 * @param {Array} urns array of urns
 * @returns {Array} array of sorted unique filter triplets ([key, op, value])
 */
export function toFilters(urns) {
  const flatten = (agg, l) => agg.concat(l);
  const dimensionFilters = filterPrefix(urns, 'thirdeye:dimension:').map(urn => _.slice(urn.split(':').map(decodeURIComponent), 2, 4).insertAt(1, '='));
  const dimensionsFilters = filterPrefix(urns, 'thirdeye:dimensions:').map(extractTail).map(enc => enc.map(tup => splitFilterFragment(decodeURIComponent(tup)))).reduce(flatten, []);
  const metricFilters = filterPrefix(urns, 'thirdeye:metric:').map(extractTail).map(enc => enc.map(tup => splitFilterFragment(decodeURIComponent(tup)))).reduce(flatten, []);
  const frontendMetricFilters = filterPrefix(urns, 'frontend:metric:').map(extractTail).map(enc => enc.map(tup => splitFilterFragment(decodeURIComponent(tup)))).reduce(flatten, []);
  const anomalyFunctionFilters = filterPrefix(urns, 'frontend:anomalyfunction:').map(extractTail).map(enc => enc.map(tup => splitFilterFragment(decodeURIComponent(tup)))).reduce(flatten, []);
  const anomalyFilters = filterPrefix(urns, 'thirdeye:event:anomaly:').map(extractTail).map(enc => enc.map(tup => splitFilterFragment(decodeURIComponent(tup)))).reduce(flatten, []);
  const callgraphFilters = filterPrefix(urns, 'thirdeye:callgraph:').map(extractTail).map(enc => enc.map(tup => splitFilterFragment(decodeURIComponent(tup)))).reduce(flatten, []);

  return [...new Set([...dimensionFilters, ...dimensionsFilters, ...metricFilters, ...frontendMetricFilters, ...anomalyFunctionFilters, ...anomalyFilters, ...callgraphFilters])].sort();
}

/**
 * Splits filter fragments strings into 2-tuples (e.g. 'key=value' to ['key', 'value'])
 *
 * @param {String} fragment
 * @returns {Array} filter tuples
 */
export function splitFilterFragment(fragment) {
  // Note: matching order not respected by regex, so split into two parts manually

  const reLong = /^(.+)(!=|<=|>=)(.*)$/;
  const partsLong = reLong.exec(fragment);
  if (!_.isEmpty(partsLong)) {
    return [partsLong[1], partsLong[2], partsLong[3]];
  }

  const reShort = /^(.+)(=|<|>)(.*)$/;
  const partsShort = reShort.exec(fragment);
  if (!_.isEmpty(partsShort)) {
    return [partsShort[1], partsShort[2], partsShort[3]];
  }

  throw new Error(`Unsupported fragment '${fragment}'`);
}

/**
 * Converts a filter multimap/object into an array of filter triplets [key, op, value]
 *
 * @see toFilterMap(filters)
 *
 * @param {Object} filterMap filter values, keyed by filter keys
 * @returns {Array} filter triplets
 */
export function fromFilterMap(filterMap) {
  const filters = [];
  Object.keys(filterMap).forEach(key => {
    [...filterMap[key]].forEach(value => {
      filters.push(value2filter(key, value));
    });
  });
  return filters;
}

/**
 * Converts an array of filter triplets [key, op, value] into a filter multimap/object.
 *
 * @see fromFilterMap(filterMap)
 *
 * @param {Array} filters array fo filter triplets
 * @returns {Object} multimap of filter values, keyed by filter keys
 */
export function toFilterMap(filters) {
  const filterMap = {};
  filters.forEach(t => {
    const dimName = t[0];
    if (!filterMap[dimName]) {
      filterMap[dimName] = new Set();
    }
    filterMap[dimName].add(`${filter2value(t)}`);
  });

  // Set to list
  Object.keys(filterMap).forEach(dimName => filterMap[dimName] = [...filterMap[dimName]]);

  return filterMap;
}

/**
 * Converts a single filter triplet ([key, op, value]) into a filter map value.
 *
 * @param {Array} filter filter triplet
 * @returns {string}
 */
export function filter2value(filter) {
  if (filter[1] === '=') {
    return filter[2];
  }
  if (filter[1] === '!=') {
    return `!${filter[2]}`;
  }
  return `${filter[1]}${filter[2]}`;
}

/**
 * Converts a single filter map key-value pair into a filter triplet.
 *
 * @param {string} key map entry key
 * @param {string} value map entry value
 * @returns {Array}
 */
export function value2filter(key, value) {
  if (value.startsWith('!')) {
    return [key, '!=', value.substring(1)];
  }
  if (value.startsWith('<=') || value.startsWith('>=')) {
    return [key, value.substring(0, 2), value.substring(2)];
  }
  if (value.startsWith('<') || value.startsWith('>')) {
    return [key, value.substring(0, 1), value.substring(1)];
  }
  return [key, '=', value];
}

/**
 * Returns true if the list of filters contains non-inclusion expressions.
 *
 * @param {Array} filters filter trples [key, op, value]
 * @returns {boolean}
 */
export function hasExclusionFilters(filters) {
  return !_.isEmpty(filters.map(t => t[1]).filter(op => op !== '='));
}

/**
 * Returns a color identify based on static mapping of an entity urn's id portion. Supports 'thirdeye:metric:' and 'thirdeye:event:' prefixes.
 *
 * @see colorMapping
 *
 * @param {string} urn entity urn with id
 * @returns {string} color identifier
 */
export function toColor(urn) {
  const metricColors = [
    'blue',
    'green',
    'red',
    'purple',
    'orange',
    'teal',
    'pink',
    'grey'
  ];
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
 * Returns a direction identifier for a floating point number. NaN safe.
 *
 * @see isInverse(urn, entities)
 *
 * @param {float} delta floating point delta
 * @param {boolean} inverse invert mapping
 * @returns {string} direction identifier ('negative', 'neutral', 'positive')
 */
export function toColorDirection(delta, inverse = false) {
  if (Number.isNaN(delta)) { return 'neutral'; }

  if (inverse) { delta *= -1; }

  switch (Math.sign(delta)) {
    case -1: return 'negative';
    case 0: return 'neutral';
    case 1: return 'positive';
  }
}

/**
 * Extract information about whether a metric is tagged as inverse.
 * (i.e. by default up is positive, and down 'negative', this property inverts the coloring)
 *
 * @see toColorDirection(delta, inverse)
 *
 * @param {string} urn metric urn
 * @param {Object} entities entities cache
 * @returns {boolean} whether metric changes are colored inversely
 */
export function isInverse(urn, entities) {
  try {
    return (entities[urn].attributes.inverse[0] === 'true');
  } catch (error) {
    return false;
  }
}

/**
 * Extracts information about whether a metric is tagged as additive or not.
 *
 * @param {string} urn metric urn
 * @param {Object} entities entities cache
 * @returns {boolean} if metric is tagged as additive
 */
export function isAdditive(urn, entities) {
  try {
    return (entities[urn].attributes.additive[0] === 'true');
  } catch (error) {
    return false;
  }
}

/**
 * Extracts information about non-additivity and the presence of exclusion filters
 * @param {string} urn metric urn
 * @param {object} entities entities cache
 */
export function isExclusionWarning(urn, entities) {
  try {
    return !isAdditive(urn, entities) && hasExclusionFilters(toFilters([urn]));
  } catch(error) {
    return false;
  }
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

/**
 * Returns time ranges for rootcause queries trimmed (intelligently) to the endpoint's maximum bounds.
 *
 * @param {Array} anomalyRange anomaly time range
 * @param {Array} analysisRange display time range
 * @return {Object} trimmed { anomalyRange, analysisRange }
 */
export function trimTimeRanges(anomalyRange, analysisRange) {
  // trim anomaly range from start of anomaly range forward
  const newAnomalyDuration = Math.min(anomalyRange[1] - anomalyRange[0], ROOTCAUSE_ANOMALY_DURATION_MAX);
  const newAnomalyRange = [anomalyRange[0], anomalyRange[0] + newAnomalyDuration];

  // trim analysis range from end of anomaly range backward
  const newAnalysisDuration = Math.min(analysisRange[1] - analysisRange[0], ROOTCAUSE_ANALYSIS_DURATION_MAX);
  const newAnalysisRangeStart = Math.max(analysisRange[0], anomalyRange[1] - newAnalysisDuration);
  const newAnalysisRange = [newAnalysisRangeStart, anomalyRange[1]];

  return Object.assign({}, {
    anomalyRange: newAnomalyRange,
    analysisRange: newAnalysisRange
  });
}

export default {
  isIterable,
  makeIterable,
  filterObject,
  toCurrentUrn,
  toBaselineUrn,
  toMetricUrn,
  toOffsetUrn,
  toAbsoluteUrn,
  toDimensionsUrn,
  stripTail,
  extractTail,
  appendTail,
  hasPrefix,
  filterPrefix,
  toBaselineRange,
  toAbsoluteRange,
  toFilters,
  toFilterMap,
  findLabelMapping,
  toMetricLabel,
  toMetricDataset,
  toColor,
  toColorDirection,
  toWidthNumber,
  isInverse,
  makeSortable,
  fromFilterMap,
  appendFilters,
  colorMapping,
  eventColorMapping,
  dateFormatFull,
  trimTimeRanges,
  splitFilterFragment,
  makeTime,
  filter2value,
  value2filter,
  hasExclusionFilters,
  isExclusionWarning
};

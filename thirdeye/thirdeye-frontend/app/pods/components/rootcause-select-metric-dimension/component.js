import { computed, get, set, getProperties, setProperties } from '@ember/object';
import Component from '@ember/component';
import fetch from 'fetch';
import {
  filterPrefix,
  toFilters,
  stripTail,
  toFilterMap,
  appendFilters,
  fromFilterMap,
  toBaselineUrn,
  toCurrentUrn,
  value2filter,
  isAdditive
} from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import _ from 'lodash';

/* eslint-disable ember/avoid-leaking-state-in-ember-objects */
export default Component.extend({
  /**
   * Currently selected metric urn including base metric and filters
   * @type {string}
   */
  selectedUrn: null,

  /**
   * Callback on change in selected urn
   * @type {function}
   */
  onSelection: null,

  //
  // internal
  //

  /**
   * Base metric urn (without filters)
   * @type {string}
   */
  baseUrn: null, // ""

  /**
   * Cache of most recent base metric urn. Used to prevent triggering the callback on re-selecting same metric.
   * @type {string}
   */
  baseUrnCache: null, // ""

  /**
   * Multimap (map of sets) of selected dimension values keyed by dimension name
   * @type {object}
   */
  filterMap: {},

  /**
   * Multimap (map of arrays) of available dimension names and values for the current base metric. Loaded dynamically from the backend.
   * @type {object}
   */
  // eslint-disable-next-line ember/avoid-leaking-state-in-ember-objects
  filterOptions: {},

  /**
   * Toggle (for show/hide) to force the UI to display the exclusions field even if no exclusions are selected.
   * @type {boolean}
   */
  forceShowExclusions: false,

  didReceiveAttrs() {
    this._super(...arguments);

    const { selectedUrn, baseUrnCache } = getProperties(this, 'selectedUrn', 'baseUrnCache');

    if (!selectedUrn || !selectedUrn.startsWith('thirdeye:metric:')) {
      setProperties(this, {
        baseUrn: null,
        filterMap: {},
        filterOptions: {}
      });
      return;
    }

    const baseUrn = stripTail(selectedUrn);
    const filterMap = toFilterMap(toFilters(selectedUrn));

    if (!_.isEqual(baseUrn, baseUrnCache)) {
      setProperties(this, { baseUrnCache: baseUrn });
      this._fetchFilters(baseUrn, filterMap);
    }

    setProperties(this, { baseUrn, filterMap });
  },

  /**
   * Backwards-compatible filter map string for filter-select component for inclusion filters.
   * The component only understands JSON-serialized maps of arrays of dimension names and values.
   * @type {string}
   */
  inclusions: computed('filterMap', {
    get() {
      const { filterMap } = getProperties(this, 'filterMap');
      const inclusionsOnly = fromFilterMap(filterMap).filter((t) => t[1] === '=');
      return JSON.stringify(toFilterMap(inclusionsOnly));
    },
    set(key, value) {
      const { filterMap } = getProperties(this, 'filterMap');
      const withoutInclusions = fromFilterMap(filterMap).filter((t) => t[1] !== '=');
      const inclusions = fromFilterMap(JSON.parse(value));
      setProperties(this, { filterMap: toFilterMap([...inclusions, ...withoutInclusions]) });
    }
  }),

  /**
   * Backwards-compatible filter map string for filter-select component for exclusions.
   * The component only understands JSON-serialized maps of arrays of dimension names and values.
   * We therefore have to filter and translate negated values to to positive one (e.g. "!us" to "us")
   * and back when setting the value.
   * @type {string}
   */
  exclusions: computed('filterMap', {
    get() {
      const { filterMap } = getProperties(this, 'filterMap');
      const exclusionsOnly = fromFilterMap(filterMap).filter((t) => t[1] === '!=');
      const exclusionsAsInclusions = exclusionsOnly.map((t) => [t[0], '=', t[2]]);
      return JSON.stringify(toFilterMap(exclusionsAsInclusions));
    },
    set(key, value) {
      const { filterMap } = getProperties(this, 'filterMap');
      const withoutExclusions = fromFilterMap(filterMap).filter((t) => t[1] !== '!=');
      const exclusionsAsInclusions = fromFilterMap(JSON.parse(value));
      const exclusions = exclusionsAsInclusions.map((t) => [t[0], '!=', t[2]]);
      setProperties(this, { filterMap: toFilterMap([...exclusions, ...withoutExclusions]) });
    }
  }),

  /**
   * Flag for selected urn with and without exclusion filters.\
   * @type {boolean}
   */
  hasExclusions: computed('exclusions', function () {
    return get(this, 'exclusions') !== '{}';
  }),

  /**
   * Flag for displaying or hiding the exclusions filter input box
   * @type {boolean}
   */
  showExclusions: computed.or('hasExclusions', 'forceShowExclusions'),

  /**
   * Flag for displaying warning icon next to exclusion filter box on non-additive metric
   * @type {boolean}
   */
  isExclusionWarning: computed('selectedUrn', 'entities', function () {
    const { selectedUrn, entities } = getProperties(this, 'selectedUrn', 'entities');
    return selectedUrn in entities && !isAdditive(selectedUrn, entities);
  }),

  /**
   * Prunes the currently selected filter map based on the dimension names and values supported
   * by the newly-selected base metric urn.
   *
   * @param {object} filterOptions map of arrays of available dimension names to dimension values
   * @param {object} filterMap map of sets of selected dimension names to dimension values
   * @private
   */
  _pruneFilters(filterOptions, filterMap) {
    const newFilterMap = {};

    Object.keys(filterMap).forEach((key) => {
      const options = new Set(filterOptions[key] || []);
      newFilterMap[key] = new Set();

      filterMap[key].forEach((value) => {
        const filter = value2filter(key, value);
        if (options.has(filter[2])) {
          newFilterMap[key].add(value);
        }
      });
    });

    return newFilterMap;
  },

  /**
   * Retrieves a map of arrays of dimension names and values for the currently selected base metric urn
   * and prunes the selected filters based on available filters.
   *
   * @param {string} baseUrn currently selected base metric urn
   * @param {object} filterMap map of sets of selected dimension names to dimension values
   * @returns {boolean}
   * @private
   */
  _fetchFilters(baseUrn, filterMap) {
    if (!baseUrn) {
      return;
    }

    const id = baseUrn.split(':')[2];

    return fetch(`/data/autocomplete/filters/metric/${id}`)
      .then(checkStatus)
      .then((res) => setProperties(this, { filterOptions: res, filterMap: this._pruneFilters(res, filterMap) }))
      .then(() => this.send('onSelect'));
  },

  actions: {
    onMetric(updates) {
      const metricUrns = filterPrefix(Object.keys(updates), 'thirdeye:metric:');

      if (_.isEmpty(metricUrns)) {
        return;
      }

      const baseUrn = metricUrns[0];

      const { selectedUrn, baseUrnCache } = getProperties(this, 'selectedUrn', 'baseUrnCache');

      const previousFilterMap = toFilterMap(toFilters(selectedUrn));
      const incomingFilterMap = toFilterMap(toFilters(baseUrn));
      const newFilterMap = _.isEmpty(incomingFilterMap) ? previousFilterMap : incomingFilterMap;

      if (!_.isEqual(baseUrn, baseUrnCache) || !_.isEqual(previousFilterMap, newFilterMap)) {
        setProperties(this, { baseUrn, baseUrnCache: baseUrn });
        this._fetchFilters(baseUrn, newFilterMap);
      }
    },

    onSelect() {
      const { baseUrn, filterMap, selectedUrn, onSelection } = getProperties(
        this,
        'baseUrn',
        'filterMap',
        'selectedUrn',
        'onSelection'
      );

      const metricUrn = appendFilters(baseUrn, fromFilterMap(filterMap));

      // only trigger on actual metric/filter change
      if (_.isEqual(metricUrn, selectedUrn)) {
        return;
      }

      const updates = { [metricUrn]: true, [toBaselineUrn(metricUrn)]: true, [toCurrentUrn(metricUrn)]: true };

      onSelection(updates);
    },

    showExclusions() {
      set(this, 'forceShowExclusions', true);
    },

    hideExclusions() {
      set(this, 'forceShowExclusions', false);
    }
  }
});

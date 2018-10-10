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
  value2filter
} from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import _ from 'lodash';

export default Component.extend({
  selectedUrn: null,

  onSelection: null,

  //
  // internal
  //
  baseUrn: null, // ""

  baseUrnCache: null, // ""

  filterMap: {},

  filterOptions: {},

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

  inclusions: computed('filterMap', {
    get () {
      const { filterMap } = getProperties(this, 'filterMap');
      const inclusionsOnly = fromFilterMap(filterMap).filter(t => t[1] === '=');
      return JSON.stringify(toFilterMap(inclusionsOnly));
    },
    set (key, value) {
      const { filterMap } = getProperties(this, 'filterMap');
      const withoutInclusions = fromFilterMap(filterMap).filter(t => t[1] !== '=');
      const inclusions = fromFilterMap(JSON.parse(value));
      setProperties(this, { filterMap: toFilterMap([...inclusions, ...withoutInclusions])});
    }
  }),

  exclusions: computed('filterMap', {
    get () {
      const { filterMap } = getProperties(this, 'filterMap');
      const exclusionsOnly = fromFilterMap(filterMap).filter(t => t[1] === '!=');
      const exclusionsAsInclusions = exclusionsOnly.map(t => [t[0], '=', t[2]]);
      return JSON.stringify(toFilterMap(exclusionsAsInclusions));
    },
    set (key, value) {
      const { filterMap } = getProperties(this, 'filterMap');
      const withoutExclusions = fromFilterMap(filterMap).filter(t => t[1] !== '!=');
      const exclusionsAsInclusions = fromFilterMap(JSON.parse(value));
      const exclusions = exclusionsAsInclusions.map(t => [t[0], '!=', t[2]]);
      setProperties(this, { filterMap: toFilterMap([...exclusions, ...withoutExclusions])});
    }
  }),

  hasExclusions: computed('exclusions', function () {
    return get(this, 'exclusions') !== '{}';
  }),

  showExclusions: computed.or('hasExclusions', 'forceShowExclusions'),

  _pruneFilters(filterOptions, filterMap) {
    const newFilterMap = {};

    Object.keys(filterMap).forEach(key => {
      const options = new Set(filterOptions[key] || []);
      newFilterMap[key] = new Set();

      filterMap[key].forEach(value => {
        const filter = value2filter(key, value);
        if (options.has(filter[2])) {
          newFilterMap[key].add(value);
        }
      });
    });

    return newFilterMap;
  },

  _fetchFilters(baseUrn, filterMap) {
    if (!baseUrn) { return; }

    const id = baseUrn.split(':')[2];

    return fetch(`/data/autocomplete/filters/metric/${id}`)
      .then(checkStatus)
      .then(res => setProperties(this, { filterOptions: res, filterMap: this._pruneFilters(res, filterMap) }))
      .then(() => this.send('onSelect'));
  },

  actions: {
    onMetric(updates) {
      const metricUrns = filterPrefix(Object.keys(updates), 'thirdeye:metric:');

      if (_.isEmpty(metricUrns)) { return; }

      const baseUrn = metricUrns[0];

      const { selectedUrn, baseUrnCache } = getProperties(this, 'selectedUrn', 'baseUrnCache');
      const filterMap = toFilterMap(toFilters(selectedUrn));

      if (!_.isEqual(baseUrn, baseUrnCache)) {
        setProperties(this, { baseUrn, baseUrnCache: baseUrn });
        this._fetchFilters(baseUrn, filterMap);
      }
    },

    onSelect() {
      const { baseUrn, filterMap, selectedUrn, onSelection } =
        getProperties(this, 'baseUrn', 'filterMap', 'selectedUrn', 'onSelection');

      const metricUrn = appendFilters(baseUrn, fromFilterMap(filterMap));

      // only trigger on actual metric/filter change
      if (_.isEqual(metricUrn, selectedUrn)) { return; }

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

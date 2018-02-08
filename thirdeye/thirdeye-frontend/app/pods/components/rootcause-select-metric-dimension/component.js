import Ember from 'ember';
import fetch from 'fetch';
import { filterPrefix, toFilters, stripTail, toFilterMap, appendFilters, fromFilterMap, toBaselineUrn, toCurrentUrn } from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import _ from 'lodash';

export default Ember.Component.extend({
  selectedUrn: null,

  onSelection: null,

  //
  // internal
  //
  baseUrn: null, // ""

  baseUrnCache: null, // ""

  filterMap: {},

  filterOptions: {},

  didReceiveAttrs() {
    this._super(...arguments);

    const { selectedUrn, baseUrnCache } = this.getProperties('selectedUrn', 'baseUrnCache');

    if (!selectedUrn || !selectedUrn.startsWith('thirdeye:metric:')) {
      this.setProperties({
        baseUrn: null,
        filterMap: {},
        filterOptions: {}
      });
      return;
    }

    const baseUrn = stripTail(selectedUrn);
    const filterMap = toFilterMap(toFilters(selectedUrn));

    if (!_.isEqual(baseUrn, baseUrnCache)) {
      this.setProperties({ baseUrnCache: baseUrn });
      this._fetchFilters(baseUrn, filterMap);
    }

    this.setProperties({ baseUrn, filterMap });
  },

  filters: Ember.computed('filterMap', {
    get() {
      const { filterMap } = this.getProperties('filterMap');
      return JSON.stringify(filterMap);
    },
    set() {
      // ignore, prevent CP override by filter-select
    }
  }),

  _pruneFilters(filterOptions, filterMap) {
    const newFilterMap = {};

    Object.keys(filterMap).forEach(key => {
      const options = new Set(filterOptions[key] || []);
      newFilterMap[key] = new Set();

      filterMap[key].forEach(value => {
        if (options.has(value)) {
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
      .then(res => this.setProperties({ filterOptions: res, filterMap: this._pruneFilters(res, filterMap) }))
      .then(() => this.send('onSelect'));
  },

  actions: {
    onMetric(updates) {
      const metricUrns = filterPrefix(Object.keys(updates), 'thirdeye:metric:');

      if (_.isEmpty(metricUrns)) { return; }

      const baseUrn = metricUrns[0];

      this.setProperties({ baseUrn });
      this.send('onSelect');
    },

    onFilters(filters) {
      const filterMap = JSON.parse(filters);

      this.setProperties({ filterMap });
      this.send('onSelect');
    },

    onSelect() {
      const { baseUrn, filterMap, selectedUrn, onSelection } =
        this.getProperties('baseUrn', 'filterMap', 'selectedUrn', 'onSelection');

      const metricUrn = appendFilters(baseUrn, fromFilterMap(filterMap));

      // only trigger on actual metric/filter change
      if (_.isEqual(metricUrn, selectedUrn)) { return; }

      const updates = { [metricUrn]: true, [toBaselineUrn(metricUrn)]: true, [toCurrentUrn(metricUrn)]: true };

      onSelection(updates);
    }
  }
});

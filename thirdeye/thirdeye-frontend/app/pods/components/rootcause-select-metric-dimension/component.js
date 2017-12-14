import Ember from 'ember';
import fetch from 'fetch';
import { filterPrefix, checkStatus, toFilters, stripTail, toFilterMap, appendFilters, fromFilterMap, toBaselineUrn, toCurrentUrn } from 'thirdeye-frontend/helpers/utils';
import _ from 'lodash'

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
      this._fetchFilters(baseUrn);
      this.setProperties({ baseUrnCache: baseUrn });
    }

    this.setProperties({ baseUrn, filterMap });
  },

  filters: Ember.computed('filterMap', {
    get() {
      const { filterMap } = this.getProperties('filterMap');
      return JSON.stringify(filterMap);
    },
    set() {
      // ignore
    }
  }),

  _pruneFilters(filterOptions) {
    const { filterMap } = this.getProperties('filterMap');

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

  _fetchFilters(baseUrn) {
    if (!baseUrn) { return; }

    const id = baseUrn.split(':')[2];

    return fetch(`/data/autocomplete/filters/metric/${id}`)
        .then(checkStatus)
        .then(res => this.setProperties({ filterOptions: res, filterMap: this._pruneFilters(res) }));
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
      const { baseUrn, filterMap, onSelection } =
        this.getProperties('baseUrn', 'filterMap', 'onSelection');

      const metricUrn = appendFilters(baseUrn, fromFilterMap(filterMap));

      const updates = { [metricUrn]: true, [toBaselineUrn(metricUrn)]: true, [toCurrentUrn(metricUrn)]: true };

      onSelection(updates);
    }
  }
});

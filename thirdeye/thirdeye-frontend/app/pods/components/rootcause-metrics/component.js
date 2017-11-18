import Ember from 'ember';
import { toCurrentUrn, toBaselineUrn, hasPrefix, filterPrefix } from '../../../helpers/utils';

const ROOTCAUSE_METRICS_SORT_PROPERTY_METRIC = 'metric';
const ROOTCAUSE_METRICS_SORT_PROPERTY_DATASET = 'dataset';
const ROOTCAUSE_METRICS_SORT_PROPERTY_CHANGE = 'change';

const ROOTCAUSE_METRICS_SORT_MODE_ASC = 'asc';
const ROOTCAUSE_METRICS_SORT_MODE_DESC = 'desc';

export default Ember.Component.extend({
  entities: null, // {}

  aggregates: null, // {}

  selectedUrns: null, // Set

  onSelection: null, // function (Set, state)

  sortProperty: null, // ""

  sortMode: null, // ""

  init() {
    this._super(...arguments);
    this.setProperties({ sortProperty: ROOTCAUSE_METRICS_SORT_PROPERTY_CHANGE, sortMode: ROOTCAUSE_METRICS_SORT_MODE_ASC });
  },

  urns: Ember.computed(
    'entities',
    'metrics',
    'datasets',
    'changes',
    'sortProperty',
    'sortMode',
    function () {
      const { entities, metrics, datasets, changes, sortProperty, sortMode } =
        this.getProperties('entities', 'metrics', 'datasets', 'changes', 'sortProperty', 'sortMode');

      const metricUrns = filterPrefix(Object.keys(entities), ['thirdeye:metric:']);
      let output = [];

      if (sortProperty == ROOTCAUSE_METRICS_SORT_PROPERTY_METRIC) {
        output = metricUrns.map(urn => [metrics[urn], urn]).sort().map(t => t[1]);
      }

      if (sortProperty == ROOTCAUSE_METRICS_SORT_PROPERTY_DATASET) {
        output = metricUrns.map(urn => [datasets[urn], urn]).sort().map(t => t[1]);
      }

      if (sortProperty == ROOTCAUSE_METRICS_SORT_PROPERTY_CHANGE) {
        output = metricUrns.map(urn => [changes[urn], urn]).sort((a, b) => parseFloat(a) - parseFloat(b)).map(t => t[1]);
      }

      if (sortMode == ROOTCAUSE_METRICS_SORT_MODE_DESC) {
        output = output.reverse();
      }

      return output;
    }
  ),

  metrics: Ember.computed(
    'entities',
    function () {
      const { entities } = this.getProperties('entities');
      return filterPrefix(Object.keys(entities), ['thirdeye:metric:'])
        .reduce((agg, urn) => {
          agg[urn] = entities[urn].label.split('::')[1].split("_").join(' ');
          return agg;
        }, {});
    }
  ),

  datasets: Ember.computed(
    'entities',
    function () {
      const { entities } = this.getProperties('entities');
      return filterPrefix(Object.keys(entities), ['thirdeye:metric:'])
        .reduce((agg, urn) => {
          agg[urn] = entities[urn].label.split('::')[0].split("_").join(' ');
          return agg;
        }, {});
    }
  ),

  changes: Ember.computed(
    'entities',
    'aggregates',
    function () {
      const { entities, aggregates } = this.getProperties('entities', 'aggregates');
      return filterPrefix(Object.keys(entities), ['thirdeye:metric:'])
        .reduce((agg, urn) => {
          agg[urn] = aggregates[toCurrentUrn(urn)] / aggregates[toBaselineUrn(urn)] - 1;
          return agg;
        }, {});
    }
  ),

  changesFormatted: Ember.computed(
    'changes',
    function () {
      const { changes } = this.getProperties('changes');
      return Object.keys(changes).reduce((agg, urn) => {
        const value = changes[urn];
        const sign = value > 0 ? '+' : '';
        if (Math.abs(value) > 5) {
          agg[urn] = 'spike';
        } else {
          agg[urn] = sign + (value * 100).toFixed(2) + '%';
        }
        return agg;
      }, {});
    }
  ),

  actions: {
    toggleSelection(urn) {
      const { selectedUrns, onSelection } = this.getProperties('selectedUrns', 'onSelection');
      if (onSelection) {
        const state = !selectedUrns.has(urn);
        const updates = { [urn]: state };
        if (hasPrefix(urn, 'thirdeye:metric:')) {
          updates[toCurrentUrn(urn)] = state;
          updates[toBaselineUrn(urn)] = state;
        }
        onSelection(updates);
      }
    },

    toggleSort(property) {
      const { sortProperty, sortMode } = this.getProperties('sortProperty', 'sortMode');
      if (property != sortProperty) {
        this.setProperties({ sortProperty: property, sortMode: ROOTCAUSE_METRICS_SORT_MODE_ASC });
      } else {
        const newSortMode = sortMode == ROOTCAUSE_METRICS_SORT_MODE_ASC ?
          ROOTCAUSE_METRICS_SORT_MODE_DESC : ROOTCAUSE_METRICS_SORT_MODE_ASC;
        this.setProperties({ sortMode: newSortMode });
      }
    }
  }
});

import Ember from 'ember';
import { toBaselineUrn, hasPrefix, filterPrefix } from '../../../helpers/utils';

const ROOTCAUSE_METRICS_SORT_METRIC = 'metric';
const ROOTCAUSE_METRICS_SORT_DATASET = 'dataset';
const ROOTCAUSE_METRICS_SORT_CHANGE = 'change';

export default Ember.Component.extend({
  entities: null, // {}

  aggregates: null, // {}

  selectedUrns: null, // Set

  onSelection: null, // function (Set, state)

  sortMode: null, // ""

  init() {
    this._super(...arguments);
    this.setProperties({ sortMode: ROOTCAUSE_METRICS_SORT_CHANGE });
  },

  urns: Ember.computed(
    'entities',
    'metrics',
    'datasets',
    'changes',
    'sortMode',
    function () {
      const { entities, metrics, datasets, changes, sortMode } =
        this.getProperties('entities', 'metrics', 'datasets', 'changes', 'sortMode');

      const funcScore = this._makeScoreFunc(sortMode);
      const metricUrns = filterPrefix(Object.keys(entities), ['thirdeye:metric:']);
      return metricUrns
        .map(urn => [funcScore(metrics[urn], datasets[urn], changes[urn]), urn])
        // .sort((a, b) => { if (parseFloat(a) < parseFloat(b)) { return -1; } else if (parseFloat(a) > parseFloat(b)) { return 1; } else { return 0; }})
        .sort()
        .map(t => t[1]);
    }
  ),

  metrics: Ember.computed(
    'entities',
    function () {
      const { entities } = this.getProperties('entities');
      const metricUrns = filterPrefix(Object.keys(entities), ['thirdeye:metric:']);
      return metricUrns.reduce((agg, urn) => {
        agg[urn] = entities[urn].label.split('::')[1].split("_").join(' ');
        return agg;
      }, {});
    }
  ),

  datasets: Ember.computed(
    'entities',
    function () {
      const { entities } = this.getProperties('entities');
      const metricUrns = filterPrefix(Object.keys(entities), ['thirdeye:metric:']);
      return metricUrns.reduce((agg, urn) => {
        agg[urn] = entities[urn].label.split('::')[0].split("_").join(' ');
        return agg;
      }, {});
    }
  ),

  changes: Ember.computed(
    'aggregates',
    function () {
      const { aggregates } = this.getProperties('aggregates');

      console.log('rootcauseMetrics: changes: aggregates', aggregates);
      const metricUrns = filterPrefix(Object.keys(aggregates), ['thirdeye:metric:']);

      return metricUrns
        .filter(urn => aggregates[toBaselineUrn(urn)])
        .reduce((agg, urn) => {
          agg[urn] = aggregates[urn] / aggregates[toBaselineUrn(urn)] - 1;
          return agg;
        }, {});
    }
  ),

  changesFormatted: Ember.computed(
    'changes',
    function () {
      const { changes } = this.getProperties('changes');
      return Object.keys(changes).reduce((agg, urn) => {
        const sign = changes[urn] > 0 ? '+' : '';
        agg[urn] = sign + (changes[urn] * 100).toFixed(2) + '%';
        return agg;
      }, {});
    }
  ),

  _makeScoreFunc(sortMode) {
    switch (sortMode) {
      case ROOTCAUSE_METRICS_SORT_METRIC:
        return (m, d, c) => m;
      case ROOTCAUSE_METRICS_SORT_DATASET:
        return (m, d, c) => d;
      case ROOTCAUSE_METRICS_SORT_CHANGE:
        return (m, d, c) => c;
    }
    return (m, d, c) => 1;
  },

  actions: {
    toggleSelection(urn) {
      const { selectedUrns, onSelection } = this.getProperties('selectedUrns', 'onSelection');
      if (onSelection) {
        const state = !selectedUrns.has(urn);
        const updates = { [urn]: state };
        if (hasPrefix(urn, 'thirdeye:metric:')) {
          updates[toBaselineUrn(urn)] = state;
        }
        onSelection(updates);
      }
    }
  }
});

import Ember from 'ember';
import { toBaselineUrn, hasPrefix, filterPrefix } from '../../../helpers/utils';

export default Ember.Component.extend({
  entities: null, // {}

  aggregates: null, // {}

  selectedUrns: null, // Set

  onSelection: null, // function (Set, state)

  labels: Ember.computed(
    'entities',
    function () {
      const { entities } = this.getProperties('entities');

      const metricUrns = filterPrefix(Object.keys(entities), ['thirdeye:metric:']);
      return metricUrns.reduce((agg, urn) => { agg[urn] = entities[urn].label; return agg; }, {});
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
        .reduce((agg, urn) => { agg[urn] = aggregates[urn] / aggregates[toBaselineUrn(urn)] - 1; return agg; }, {});
    }
  ),

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

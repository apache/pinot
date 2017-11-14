import Ember from 'ember';
import { toBaselineUrn, filterPrefix, hasPrefix } from '../../../helpers/utils';

export default Ember.Component.extend({
  entities: null, // {}

  selectedUrns: null, // Set

  invisibleUrns: null, // Set

  onVisibility: null, // function (Set, bool)

  onSelection: null, // function (Set, bool)

  labels: Ember.computed(
    'entities',
    'selectedUrns',
    function () {
      const { entities, selectedUrns } = this.getProperties('entities', 'selectedUrns');
      const labels = {};
      [...selectedUrns].filter(urn => hasPrefix(urn, 'thirdeye:')).filter(urn => entities[urn]).forEach(urn => labels[urn] = entities[urn].label);
      return labels;
    }
  ),

  sortedUrns: Ember.computed(
    'entities',
    'selectedUrns',
    function () {
      const { entities, selectedUrns } = this.getProperties('entities', 'selectedUrns');
      return [...selectedUrns]
        .filter(urn => entities[urn])
        .map(urn => [entities[urn].label.split("::")[1], urn])
        .sort()
        .map(t => t[1]);
    }
  ),

  metrics: Ember.computed(
    'entities',
    'sortedUrns',
    function () {
      const { entities, sortedUrns } = this.getProperties('entities', 'sortedUrns');
      return filterPrefix(sortedUrns, 'thirdeye:metric:').reduce((agg, urn) => { agg[urn] = entities[urn].label.split("::")[1]; return agg; }, {});
    }
  ),

  events: Ember.computed(
    'entities',
    'sortedUrns',
    function () {
      const { entities, sortedUrns } = this.getProperties('entities', 'sortedUrns');
      return filterPrefix(sortedUrns, 'thirdeye:event:').reduce((agg, urn) => { agg[urn] = entities[urn].label; return agg; }, {});
    }
  ),

  hasMetrics: Ember.computed(
    'metrics',
    function () {
      return Object.keys(this.get('metrics')).length > 0;
    }
  ),

  hasEvents: Ember.computed(
    'events',
    function () {
      return Object.keys(this.get('events')).length > 0;
    }
  ),

  _bulkVisibility(visible, other) {
    const { onVisibility } = this.getProperties('onVisibility');
    const updates = {};
    [...visible].forEach(urn => updates[urn] = true);
    [...other].forEach(urn => updates[urn] = false);
    if (onVisibility) {
      onVisibility(updates);
    }
  },

  actions: {
    toggleVisibility(urn) {
      const { onVisibility, invisibleUrns } = this.getProperties('onVisibility', 'invisibleUrns');
      if (onVisibility) {
        const state = invisibleUrns.has(urn);
        const updates = { [urn]: state };
        if (hasPrefix(urn, 'thirdeye:metric:')) {
          updates[toBaselineUrn(urn)] = state;
        }
        onVisibility(updates);
      }
    },

    removeUrn(urn) {
      const { onSelection } = this.getProperties('onSelection');
      if (onSelection) {
        const updates = { [urn]: false };
        if (hasPrefix(urn, 'thirdeye:metric:')) {
          updates[toBaselineUrn(urn)] = false;
        }
        onSelection(updates);
      }
    },

    visibleMetrics() {
      const { selectedUrns } = this.getProperties('selectedUrns');
      const visible = new Set(filterPrefix(selectedUrns, ['thirdeye:metric:', 'frontend:baseline:metric:']));
      const other = new Set([...selectedUrns].filter(urn => !visible.has(urn)));
      this._bulkVisibility(visible, other);
    },

    visibleEvents() {
      const { selectedUrns } = this.getProperties('selectedUrns');
      const visible = new Set(filterPrefix(selectedUrns, 'thirdeye:event:'));
      const other = new Set([...selectedUrns].filter(urn => !visible.has(urn)));
      this._bulkVisibility(visible, other);
    },

    visibleAll() {
      const { selectedUrns } = this.getProperties('selectedUrns');
      this._bulkVisibility(selectedUrns, new Set());
    },

    visibleNone() {
      const { selectedUrns } = this.getProperties('selectedUrns');
      this._bulkVisibility(new Set(), selectedUrns);
    },

    visibleInvert() {
      const { selectedUrns, invisibleUrns } = this.getProperties('selectedUrns', 'invisibleUrns');
      const visible = new Set(invisibleUrns);
      const other = new Set([...selectedUrns].filter(urn => !visible.has(urn)));
      this._bulkVisibility(visible, other);
    }
  }
});

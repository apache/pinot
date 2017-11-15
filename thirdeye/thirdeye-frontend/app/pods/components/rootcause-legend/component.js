import Ember from 'ember';
import { toCurrentUrn, toBaselineUrn, filterPrefix, stripTail, hasPrefix } from '../../../helpers/utils';
import _ from 'lodash';

export default Ember.Component.extend({
  entities: null, // {}

  selectedUrns: null, // Set

  invisibleUrns: null, // Set

  onVisibility: null, // function (Set, bool)

  onSelection: null, // function (Set, bool)

  validUrns: Ember.computed(
    'entities',
    'selectedUrns',
    function () {
      const { entities, selectedUrns } = this.getProperties('entities', 'selectedUrns');
      return filterPrefix(selectedUrns, 'thirdeye:').filter(urn => entities[urn] || entities[stripTail(urn)]);
    }
  ),

  metrics: Ember.computed(
    'entities',
    'validUrns',
    function () {
      const { validUrns } = this.getProperties('validUrns');
      return filterPrefix(validUrns, 'thirdeye:metric:').reduce((agg, urn) => {
        agg[urn] = this._makeMetricLabel(urn);
        return agg;
      }, {});
    }
  ),

  events: Ember.computed(
    'entities',
    'validUrns',
    function () {
      const { entities, validUrns } = this.getProperties('entities', 'validUrns');
      return filterPrefix(validUrns, 'thirdeye:event:').reduce((agg, urn) => { agg[urn] = entities[urn].label; return agg; }, {});
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

  _makeMetricLabel(urn) {
    const { entities } = this.getProperties('entities');

    const metricName = entities[stripTail(urn)].label.split("::")[1];
    const parts = urn.split(':');

    if (parts.length <= 3) {
      return metricName;
    }

    const tail = _.slice(parts, 3);
    return metricName + ' (' + tail.join(', ') + ')';
  },

  actions: {
    toggleVisibility(urn) {
      const { onVisibility, invisibleUrns } = this.getProperties('onVisibility', 'invisibleUrns');
      if (onVisibility) {
        const state = invisibleUrns.has(urn);
        const updates = { [urn]: state };
        if (hasPrefix(urn, 'thirdeye:metric:')) {
          updates[toCurrentUrn(urn)] = state;
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
          updates[toCurrentUrn(urn)] = false;
          updates[toBaselineUrn(urn)] = false;
        }
        onSelection(updates);
      }
    },

    visibleMetrics() {
      const { selectedUrns } = this.getProperties('selectedUrns');
      const visible = new Set(filterPrefix(selectedUrns, ['thirdeye:metric:', 'frontend:metric:']));
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

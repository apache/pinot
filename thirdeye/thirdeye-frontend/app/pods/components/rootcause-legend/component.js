import { computed, get, getProperties } from '@ember/object';
import Component from '@ember/component';
import {
  toCurrentUrn,
  toBaselineUrn,
  toMetricUrn,
  filterPrefix,
  hasPrefix,
  toMetricLabel,
  toEventLabel,
  isExclusionWarning
} from 'thirdeye-frontend/utils/rca-utils';

export default Component.extend({
  entities: null, // {}

  selectedUrns: null, // Set

  invisibleUrns: null, // Set

  onVisibility: null, // function (updates)

  onSelection: null, // function (updates)

  onPrimaryChange: null, // function (updates)

  classNames: ['rootcause-legend'],

  metrics: computed('entities', 'selectedUrns', function () {
    const { selectedUrns, entities } = getProperties(this, 'selectedUrns', 'entities');
    return filterPrefix(selectedUrns, 'thirdeye:metric:').reduce((agg, urn) => {
      agg[urn] = toMetricLabel(urn, entities);
      return agg;
    }, {});
  }),

  /**
   * Parses the validUrns and builds out
   * a Mapping of event Types to a mapping of urns
   * @type {Object}
   */
  events: computed('entities', 'selectedUrns', function () {
    const { entities, selectedUrns } = getProperties(this, 'entities', 'selectedUrns');
    return filterPrefix(selectedUrns, 'thirdeye:event:').reduce((agg, urn) => {
      const type = urn.split(':')[2];
      agg[type] = agg[type] || {};
      Object.assign(agg[type], {
        [urn]: toEventLabel(urn, entities)
      });

      return agg;
    }, {});
  }),

  colors: computed('entities', 'selectedUrns', function () {
    const { entities, selectedUrns } = getProperties(this, 'entities', 'selectedUrns');
    return [...selectedUrns]
      .filter((urn) => entities[urn])
      .reduce((agg, urn) => {
        agg[urn] = entities[urn].color;
        return agg;
      }, {});
  }),

  hasMetrics: computed('metrics', function () {
    return Object.keys(get(this, 'metrics')).length > 0;
  }),

  hasEvents: computed('events', function () {
    return Object.keys(get(this, 'events')).length > 0;
  }),

  isExclusionWarning: computed('metrics', 'entities', function () {
    const { metrics, entities } = getProperties(this, 'metrics', 'entities');
    return Object.keys(metrics).reduce((agg, urn) => {
      agg[urn] = isExclusionWarning(urn, entities);
      return agg;
    }, {});
  }),

  _bulkVisibility(visible, other) {
    const { onVisibility } = getProperties(this, 'onVisibility');
    const updates = {};
    [...visible].forEach((urn) => (updates[urn] = true));
    [...other].forEach((urn) => (updates[urn] = false));
    if (onVisibility) {
      onVisibility(updates);
    }
  },

  /**
   * The inject method for handling legend hover
   * @public
   */
  onMouseEnter: () => {},

  /**
   * The inject method for handling legend leave
   * @public
   */
  onMouseLeave: () => {},

  actions: {
    /**
     * Handles the on Mouse enter event for each legend items
     * @param {String} urn
     * @returns undefined
     */
    _onMouseEnter(urn) {
      this.onMouseEnter([urn]);
    },

    /**
     * Handles the on Mouse leave event for each legend items
     * @returns undefined
     */
    _onMouseLeave() {
      this.onMouseLeave([]);
    },

    toggleVisibility(urn) {
      const { onVisibility, invisibleUrns } = getProperties(this, 'onVisibility', 'invisibleUrns');
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
      const { onSelection } = getProperties(this, 'onSelection');
      if (onSelection) {
        const updates = { [urn]: false };
        if (hasPrefix(urn, 'thirdeye:metric:')) {
          updates[toCurrentUrn(urn)] = false;
          updates[toBaselineUrn(urn)] = false;
        }
        onSelection(updates);
      }
    },

    onSelect(urn) {
      const { onPrimaryChange } = getProperties(this, 'onPrimaryChange');
      if (onPrimaryChange) {
        onPrimaryChange({ [toMetricUrn(urn)]: true, [toBaselineUrn(urn)]: true, [toCurrentUrn(urn)]: true });
      }
    }
  }
});

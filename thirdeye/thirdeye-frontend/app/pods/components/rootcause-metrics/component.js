import Ember from 'ember';
import { toCurrentUrn, toBaselineUrn, toOffsetUrn, hasPrefix, filterPrefix, toMetricLabel, makeSortable, humanizeChange, isInverse, toColorDirection } from 'thirdeye-frontend/helpers/utils';

const ROOTCAUSE_METRICS_SORT_PROPERTY_METRIC = 'metric';
const ROOTCAUSE_METRICS_SORT_PROPERTY_DATASET = 'dataset';
const ROOTCAUSE_METRICS_SORT_PROPERTY_CHANGE = 'change';
const ROOTCAUSE_METRICS_SORT_PROPERTY_SCORE = 'score';

const ROOTCAUSE_METRICS_SORT_MODE_ASC = 'asc';
const ROOTCAUSE_METRICS_SORT_MODE_DESC = 'desc';

export default Ember.Component.extend({
  entities: null, // {}

  aggregates: null, // {}

  selectedUrns: null, // Set

  onSelection: null, // function (Set, state)

  sortProperty: ROOTCAUSE_METRICS_SORT_PROPERTY_SCORE,

  sortMode: ROOTCAUSE_METRICS_SORT_MODE_DESC,

  /**
   * Currently selected view within the metrics tab
   * @type {String}
   */
  selectedView: 'card',

  /**
   * loading status for component
   */
  isLoading: false,

  /**
   * List of metric urns, sorted by sortMode.
   */
  urns: Ember.computed(
    'entities',
    'metrics',
    'datasets',
    'changes',
    'sortProperty',
    'sortMode',
    function () {
      const { entities, metrics, datasets, changes, scores, sortProperty, sortMode } =
        this.getProperties('entities', 'metrics', 'datasets', 'changes', 'scores', 'sortProperty', 'sortMode');

      const metricUrns = filterPrefix(Object.keys(entities), ['thirdeye:metric:']);
      let output = [];

      switch(sortProperty) {
        case ROOTCAUSE_METRICS_SORT_PROPERTY_METRIC:
          output = metricUrns.map(urn => [metrics[urn], urn]).sort();
          break;

        case ROOTCAUSE_METRICS_SORT_PROPERTY_DATASET:
          output = metricUrns.map(urn => [datasets[urn], urn]).sort();
          break;

        case ROOTCAUSE_METRICS_SORT_PROPERTY_CHANGE:
          output = metricUrns.map(urn => [makeSortable(changes[urn]), urn]).sort((a, b) => a[0] - b[0]);
          break;

        case ROOTCAUSE_METRICS_SORT_PROPERTY_SCORE:
          output = metricUrns.map(urn => [makeSortable(scores[urn]), urn]).sort((a, b) => a[0] - b[0]);
          break;
      }

      if (sortMode === ROOTCAUSE_METRICS_SORT_MODE_DESC) {
        output = output.reverse();
      }

      return output.map(t => t[1]);
    }
  ),

  /**
   * Metric labels, keyed by urn
   */
  metrics: Ember.computed(
    'entities',
    function () {
      const { entities } = this.getProperties('entities');
      return filterPrefix(Object.keys(entities), ['thirdeye:metric:'])
        .reduce((agg, urn) => {
          agg[urn] = toMetricLabel(urn, entities);
          return agg;
        }, {});
    }
  ),

  /**
   * Dataset labels, keyed by metric urn
   */
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

  /**
   * Change values from baseline to current time range, keyed by metric urn
   */
  changes: Ember.computed(
    'entities',
    'aggregates',
    function () {
      const { entities, aggregates } = this.getProperties('entities', 'aggregates'); // poll observer
      return this._computeChangesForOffset('baseline');
    }
  ),

  /**
   * Formatted change strings for 'changes'
   */
  changesFormatted: Ember.computed(
    'changes',
    function () {
      const { changes } = this.getProperties('changes');
      return this._formatChanges(changes);
    }
  ),

  /**
   * Change values from multiple offsets to current time range, keyed by offset, then by metric urn
   */
  changesOffset: Ember.computed(
    'entities',
    'aggregates',
    function () {
      const { entities, aggregates } = this.getProperties('entities', 'aggregates'); // poll observer

      const offsets = ['wo1w', 'wo2w', 'wo3w', 'wo4w', 'baseline'];
      const dict = {}
      offsets.forEach(offset => dict[offset] = this._computeChangesForOffset(offset));

      return dict;
    }
  ),

  /**
   * Formatted change strings for 'changesOffset'
   */
  changesOffsetFormatted: Ember.computed(
    'changesOffset',
    function () {
      const { changesOffset } = this.getProperties('changesOffset');

      const dict = {};
      Object.keys(changesOffset).forEach(offset => dict[offset] = this._formatChanges(changesOffset[offset]));

      return dict;
    }
  ),

  /**
   * Anomalyity scores, keyed by metric urn
   */
  scores: Ember.computed(
    'entities',
    function () {
      const { entities } = this.getProperties('entities');
      return filterPrefix(Object.keys(entities), ['thirdeye:metric:'])
        .reduce((agg, urn) => {
          agg[urn] = entities[urn].score.toFixed(2);
          return agg;
        }, {});
    }
  ),

  /**
   * Trend direction label (positive, neutral, negative) for change values
   */
  directions: Ember.computed(
    'entities',
    'changes',
    function () {
      const { entities, changes } = this.getProperties('entities', 'changes');

      return Object.keys(changes).reduce((agg, urn) => {
        agg[urn] = toColorDirection(changes[urn], isInverse(urn, entities));
        return agg;
      }, {});
    }
  ),

  /**
   * Compute changes from a given offset to the current time range, as a fraction.
   *
   * @param {String} offset time range offset, e.g. 'baseline', 'wow', 'wo2w', ...
   * @returns {Object} change values, keyed by metric urn
   */
  _computeChangesForOffset(offset) {
    const { entities, aggregates } = this.getProperties('entities', 'aggregates');
    return filterPrefix(Object.keys(entities), ['thirdeye:metric:'])
      .reduce((agg, urn) => {
      agg[urn] = aggregates[toCurrentUrn(urn)] / aggregates[toOffsetUrn(urn, offset)] - 1;
      return agg;
    }, {});
  },

  /**
   * Format changes dict with sign and decimals and gracefully handle outliers
   *
   * @param {Object} changes change values, keyed by metric urn
   * @returns {Object} formatted change strings
   */
  _formatChanges(changes) {
    return Object.keys(changes).reduce((agg, urn) => {
      const change = changes[urn];
      if (Number.isNaN(change)) {
        agg[urn] = '-';
        return agg;
      }

      if (Math.abs(change) > 5) {
        agg[urn] = 'spike';
        return agg;
      }

      agg[urn] = humanizeChange(change);
      return agg;
    }, {});
  },

  actions: {
    /**
     * Sets the selected view for metrics tab
     * @return {undefined}
     */
    selectView(selectedView) {
      this.setProperties({ selectedView });
    },

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

import Ember from 'ember';
import { toCurrentUrn, toBaselineUrn, toOffsetUrn, hasPrefix, filterPrefix, toMetricLabel, makeSortable, humanizeChange, isInverse, toColorDirection } from 'thirdeye-frontend/helpers/utils';

const ROOTCAUSE_METRICS_SORT_PROPERTY_METRIC = 'metric';
const ROOTCAUSE_METRICS_SORT_PROPERTY_DATASET = 'dataset';
const ROOTCAUSE_METRICS_SORT_PROPERTY_CHANGE = 'change';
const ROOTCAUSE_METRICS_SORT_PROPERTY_SCORE = 'score';

const ROOTCAUSE_METRICS_OUTPUT_MODE_ASC = 'asc';
const ROOTCAUSE_METRICS_OUTPUT_MODE_DESC = 'desc';

export default Ember.Component.extend({
  entities: null, // {}

  aggregates: null, // {}

  selectedUrns: null, // Set

  onSelection: null, // function (Set, state)

  sortProperty: ROOTCAUSE_METRICS_SORT_PROPERTY_SCORE,

  outputMode: ROOTCAUSE_METRICS_OUTPUT_MODE_DESC,

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
   * List of metric urns, sorted by sortProperty and ordered by outputMode.
   */
  urns: Ember.computed(
    'entities',
    'metrics',
    'datasets',
    'changes',
    'sortProperty',
    'outputMode',
    function () {
      const { entities, metrics, datasets, changes, scores, sortProperty, outputMode } =
        this.getProperties('entities', 'metrics', 'datasets', 'changes', 'scores', 'sortProperty', 'outputMode');

      const metricUrns = filterPrefix(Object.keys(entities), ['thirdeye:metric:']);

      // sort tuples
      const tuples = metricUrns.map(urn => Object.assign({}, {
        metric: metrics[urn],
        dataset: datasets[urn],
        change: makeSortable(changes[urn]),
        score: makeSortable(scores[urn]),
        urn
      }));

      const sortedTuples = this._sort(tuples, sortProperty);

      const outputTuples = this._output(sortedTuples, outputMode);

      return outputTuples.map(t => t.urn);
    }
  ),

  /**
   * Sorts an array of tuples depending on sortProperty.
   *
   * @param {Array} tuples sort tuples
   * @param {String} sortProperty sort property
   * @returns {Array} sorted tuples
   * @private
   */
  _sort(tuples, sortProperty) {
    switch(sortProperty) {
      case ROOTCAUSE_METRICS_SORT_PROPERTY_METRIC:
        return _.sortBy(tuples, ['metric', 'dataset']);
      case ROOTCAUSE_METRICS_SORT_PROPERTY_DATASET:
        return _.sortBy(tuples, ['dataset', 'metric']);
      case ROOTCAUSE_METRICS_SORT_PROPERTY_CHANGE:
        return _.sortBy(tuples, ['change', 'metric', 'dataset']);
      case ROOTCAUSE_METRICS_SORT_PROPERTY_SCORE:
        return _.sortBy(tuples, ['score', 'change', 'metric', 'dataset']);
      default:
        return tuples;
    }
  },

  /**
   * Returns the tuples in natural or reverse order, depending on outputMode.
   *
   * @param {Array} tuples (sorted) sort tuples
   * @param {String} outputMode output mode
   * @returns {Array} re-ordered tuples
   * @private
   */
  _output(tuples, outputMode) {
    switch(outputMode) {
      case ROOTCAUSE_METRICS_OUTPUT_MODE_ASC:
        return tuples;
      case ROOTCAUSE_METRICS_OUTPUT_MODE_DESC:
        return tuples.reverse();
      default:
        return tuples;
    }
  },

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
     */
    selectView(selectedView) {
      this.setProperties({ selectedView });
    },

    /**
     * Toggles the selection of a metric card on/off
     * @param urn
     */
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

    /**
     * Sets the sort property and toggles the output mode to asc/desc on repeated click
     * @param property
     */
    toggleSort(property) {
      const { sortProperty, outputMode } = this.getProperties('sortProperty', 'outputMode');

      // different sort property
      if (property !== sortProperty) {
        // sort asc by default, unless using score
        const newOutputMode = (property === ROOTCAUSE_METRICS_SORT_PROPERTY_SCORE) ?
          ROOTCAUSE_METRICS_OUTPUT_MODE_DESC : ROOTCAUSE_METRICS_OUTPUT_MODE_ASC;

        this.setProperties({ sortProperty: property, outputMode: newOutputMode });

      // same property, toggle output mode
      } else {
        const newOutputMode = (outputMode === ROOTCAUSE_METRICS_OUTPUT_MODE_ASC) ?
          ROOTCAUSE_METRICS_OUTPUT_MODE_DESC : ROOTCAUSE_METRICS_OUTPUT_MODE_ASC;
        this.setProperties({ outputMode: newOutputMode });
      }
    }
  }
});

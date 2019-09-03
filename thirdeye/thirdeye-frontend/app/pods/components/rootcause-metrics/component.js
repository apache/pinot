import { computed, getProperties, set } from '@ember/object';
import Component from '@ember/component';
import {
  toCurrentUrn,
  toBaselineUrn,
  toOffsetUrn,
  hasPrefix,
  filterPrefix,
  toMetricLabel,
  toMetricDataset,
  isInverse,
  toColorDirection,
  makeSortable,
  isExclusionWarning
} from 'thirdeye-frontend/utils/rca-utils';
import {
  humanizeChange,
  humanizeFloat,
  humanizeScore
} from 'thirdeye-frontend/utils/utils';
import METRICS_TABLE_COLUMNS from 'thirdeye-frontend/shared/metricsTableColumns';
import _ from 'lodash';

export default Component.extend({
  classNames: ['rootcause-metrics'],

  /**
   * Columns for metrics table
   * @type Object[]
   */
  // TODO move this to shared
  metricsTableColumns: METRICS_TABLE_COLUMNS,

  //
  // external properties
  //

  /**
   * Entities cache
   * @type {object}
   */
  entities: null,

  /**
   * Metric aggregates
   * @type {object}
   */
  aggregates: null,

  /**
   * (Metric) entity scores
   * @type {object}
   */
  scores: null,

  /**
   * User-selected urns
   * @type {Set}
   */
  selectedUrns: null,

  /**
   * Callback on metric selection
   * @type {function}
   */
  onSelection: null, // function (Set, state)

  //
  // internal properties
  //

  /**
   * A mapping of each metric and its url(s)
   * @type {Object} - key is metric urn, and value is an array of objects, each object has a key of the url label,
   * and value as the url
   * @example
   * {
   *  thirdeye:metric:12345: [],
   *  thirdeye:metric:23456: [
   *    {urlLabel: url},
   *    {urlLabel: url}
   *  ]
   * }
   */
  links: computed(
    'entities',
    function() {
      const { entities } = getProperties(this, 'entities');
      let metricUrlMapping = {};

      filterPrefix(Object.keys(entities), 'thirdeye:metric:')
        .forEach(urn => {
          const attributes = entities[urn].attributes;
          const { externalUrls = [] } = attributes;
          let urlArr = [];

          // Add the list of urls for each url type
          externalUrls.forEach(urlLabel => {
            urlArr.push({
              [urlLabel]: attributes[urlLabel][0] // each type should only have 1 url
            });
          });

          // Map all the url lists to a metric urn
          metricUrlMapping[urn] = urlArr;
        });

      return metricUrlMapping;
    }
  ),

  /**
   * Data for metrics table
   * @type Object[] - array of objects, each corresponding to a row in the table
   */
  metricsTableData: computed(
    'selectedUrns',
    'entities',
    'aggregates',
    'scores',
    'links',
    function() {
      const { selectedUrns, entities, aggregates, scores, links } =
        getProperties(this, 'selectedUrns', 'entities', 'aggregates', 'scores', 'links');

      const rows = filterPrefix(Object.keys(entities), 'thirdeye:metric:')
        .map(urn => {
          return {
            urn,
            links: links[urn],
            isSelected: selectedUrns.has(urn),
            label: toMetricLabel(urn, entities),
            dataset: toMetricDataset(urn, entities),
            score: humanizeScore(scores[urn]),
            current: this._makeRecord(urn, 'current', entities, aggregates),
            baseline: this._makeRecord(urn, 'baseline', entities, aggregates),
            wo1w: this._makeRecord(urn, 'wo1w', entities, aggregates),
            wo2w: this._makeRecord(urn, 'wo2w', entities, aggregates),
            sortable_current: this._makeChange(urn, 'current', aggregates),
            sortable_baseline: this._makeChange(urn, 'baseline', aggregates),
            sortable_wo1w: this._makeChange(urn, 'wo1w', aggregates),
            sortable_wo2w: this._makeChange(urn, 'wo2w', aggregates),
            isExclusionWarning: isExclusionWarning(urn, entities)
          };
        });

      return _.sortBy(rows, (row) => row.label);
    }
  ),

  /**
   * Returns a table record with value, change, and change direciton.
   * @param {string} urn metric urn
   * @param {string} offset offset identifier
   * @param {object} aggregates aggregates cache
   * @param {object} entities entities cache
   * @return {{value: *, change: *, direction: *}}
   * @private
   */
  _makeRecord(urn, offset, entities, aggregates) {
    const current = aggregates[toOffsetUrn(urn, 'current')] || Number.NaN;
    const value = aggregates[toOffsetUrn(urn, offset)] || Number.NaN;
    const change = current / value - 1;

    return {
      value: humanizeFloat(value),
      change: humanizeChange(change),
      direction: toColorDirection(change, isInverse(urn, entities))
    };
  },

  /**
   * Computes the relative change (as fraction) between current and offset values.
   *
   * @param {string} urn metric urn
   * @param {string} offset offset identifier
   * @param {object} aggregates aggregates cache
   * @return {double} relative change
   * @private
   */
  _makeChange(urn, offset, aggregates) {
    const current = aggregates[toOffsetUrn(urn, 'current')] || Number.NaN;
    const value = aggregates[toOffsetUrn(urn, offset)] || Number.NaN;
    return makeSortable(current / value - 1);
  },

  /**
   * Keeps track of items that are selected in the table
   * @type {Array}
   */
  preselectedItems: computed({
    get () {
      return [];
    },
    set () {
      // ignore
    }
  }),

  actions: {
    /**
     * Triggered on cell selection
     * Updates the currently selected urns based on user selection on the table
     * @param {Object} e
     */
    displayDataChanged (e) {
      if (_.isEmpty(e.selectedItems)) { return; }

      const { selectedUrns, onSelection } = getProperties(this, 'selectedUrns', 'onSelection');

      if (!onSelection) { return; }

      const urn = e.selectedItems[0].urn;
      const state = !selectedUrns.has(urn);

      const updates = {[urn]: state};
      if (hasPrefix(urn, 'thirdeye:metric:')) {
        updates[toCurrentUrn(urn)] = state;
        updates[toBaselineUrn(urn)] = state;
      }

      set(this, 'preselectedItems', []);
      onSelection(updates);
    }
  }
});

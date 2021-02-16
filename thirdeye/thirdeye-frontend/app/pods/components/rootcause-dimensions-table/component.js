import { computed, getProperties, set } from '@ember/object';
import Component from '@ember/component';
import {
  toCurrentUrn,
  toBaselineUrn,
  toMetricUrn,
  isInverse,
  toColorDirection,
  makeSortable,
  appendFilters,
  isAdditive
} from 'thirdeye-frontend/utils/rca-utils';
import { humanizeChange, humanizeFloat } from 'thirdeye-frontend/utils/utils';
import DIMENSIONS_TABLE_COLUMNS from 'thirdeye-frontend/shared/dimensionsTableColumns';
import _ from 'lodash';

const ROOTCAUSE_TRUNCATION_FRACTION = 0.0001;
const ROOTCAUSE_VALUE_OTHER = 'OTHER';

export default Component.extend({
  classNames: ['rootcause-metrics'],

  /**
   * Columns for dimensions table
   * @type {Array}
   */
  dimensionsTableColumns: DIMENSIONS_TABLE_COLUMNS,

  //
  // external properties
  //

  /**
   * Entities cache
   * @type {object}
   */
  entities: null,

  /**
   * Metric breakdowns
   * @type {object}
   */
  breakdowns: null,

  /**
   * User-selected urns
   * @type {Set}
   */
  selectedUrns: null,

  /**
   * Primary metric urn
   * @type {string}
   */
  metricUrn: null,

  /**
   * Callback on dimension selection
   * @type {function}
   */
  onSelection: null, // function (Set, state)

  //
  // internal properties
  //

  /**
   * Tracks presence of breakdown values for base metric
   * @type {boolean}
   */
  hasCurrent: computed('metricUrn', 'breakdowns', function () {
    const { metricUrn, breakdowns } = getProperties(this, 'metricUrn', 'breakdowns');
    return !_.isEmpty(breakdowns[toCurrentUrn(metricUrn)]);
  }),

  /**
   * Tracks presence of breakdown baseline for base metric
   * @type {boolean}
   */
  hasBaseline: computed('metricUrn', 'breakdowns', function () {
    const { metricUrn, breakdowns } = getProperties(this, 'metricUrn', 'breakdowns');
    return !_.isEmpty(breakdowns[toBaselineUrn(metricUrn)]);
  }),

  /**
   * Tracks additive flag state of base metric
   * @type {boolean}
   */
  isAdditive: computed('metricUrn', 'entities', function () {
    const { metricUrn, entities } = getProperties(this, 'metricUrn', 'entities');
    return isAdditive(metricUrn, entities);
  }),

  /**
   * Data for metrics table
   * @type {Array}
   */
  dimensionsTableData: computed('selectedUrns', 'entities', 'breakdowns', 'metricUrn', function () {
    const { selectedUrns, entities, breakdowns, metricUrn } = getProperties(
      this,
      'selectedUrns',
      'entities',
      'breakdowns',
      'metricUrn'
    );

    const current = breakdowns[toCurrentUrn(metricUrn)];
    const baseline = breakdowns[toBaselineUrn(metricUrn)];

    if (_.isEmpty(current) || _.isEmpty(baseline)) {
      return [];
    }

    const rows = [];

    const inverse = isInverse(metricUrn, entities);

    const contribTransform = (v) => Math.round(v * 10000) / 100.0;

    Object.keys(current).forEach((name) => {
      const currTotal = this._sum(current, name);
      const baseTotal = this._sum(baseline, name);

      Object.keys(current[name]).forEach((value) => {
        if (value === ROOTCAUSE_VALUE_OTHER) {
          return;
        }

        const urn = appendFilters(metricUrn, [[name, '=', value]]);
        const curr = (current[name] || {})[value] || 0;
        const base = (baseline[name] || {})[value] || 0;

        if (curr === 0 && base === 0) {
          return;
        }

        if (curr / currTotal < ROOTCAUSE_TRUNCATION_FRACTION && base / baseTotal < ROOTCAUSE_TRUNCATION_FRACTION) {
          return;
        }

        const change = curr / base - 1;
        const changeContribution = curr / currTotal - base / baseTotal;
        const contributionToChange = (curr - base) / baseTotal;

        rows.pushObject({
          urn,
          isSelected: selectedUrns.has(urn),
          label: `${value} (${name})`,
          current: humanizeFloat(curr),
          baseline: humanizeFloat(base),
          change: this._makeRecord(change, inverse, humanizeChange),
          changeContribution: this._makeRecord(changeContribution, inverse, contribTransform),
          contributionToChange: this._makeRecord(contributionToChange, inverse, contribTransform),
          sortable_current: makeSortable(curr),
          sortable_baseline: makeSortable(base),
          sortable_change: makeSortable(change),
          sortable_changeContribution: makeSortable(changeContribution),
          sortable_contributionToChange: makeSortable(contributionToChange)
        });
      });
    });

    return _.sortBy(rows, (row) => row.label);
  }),

  /**
   * Keeps track of items that are selected in the table
   * @type {Array}
   */
  preselectedItems: computed({
    get() {
      return [];
    },
    set() {
      // ignore
    }
  }),

  /**
   * Sums all values for a given dimension name
   *
   * @param breakdown dimension contribution breakdown
   * @param name dimension name
   * @returns {float}
   */
  _sum(breakdown, name) {
    return Object.values((breakdown || {})[name] || {}).reduce((sum, v) => sum + v || 0, 0.0);
  },

  /**
   * Generates template records for change-related columns.
   *
   * @param {float} change change amount
   * @param {boolean} inverse inverse metric flag
   * @param {function} transform string transformation
   * @returns {object}
   * @private
   */
  _makeRecord(change, inverse, transform) {
    return {
      change: transform(change),
      direction: toColorDirection(change * (inverse ? -1 : 1))
    };
  },

  actions: {
    /**
     * Triggered on cell selection
     * Updates the currently selected urns based on user selection on the table
     * @param {Object} e
     */
    displayDataChanged(e) {
      if (_.isEmpty(e.selectedItems)) {
        return;
      }

      const { selectedUrns, onSelection } = getProperties(this, 'selectedUrns', 'onSelection');

      if (!onSelection) {
        return;
      }

      const urn = e.selectedItems[0].urn;
      const state = !selectedUrns.has(urn);

      const updates = { [toMetricUrn(urn)]: state, [toCurrentUrn(urn)]: state, [toBaselineUrn(urn)]: state };

      set(this, 'preselectedItems', []);
      onSelection(updates);
    }
  }
});

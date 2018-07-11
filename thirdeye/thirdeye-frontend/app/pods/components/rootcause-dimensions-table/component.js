import { computed } from '@ember/object';
import Component from '@ember/component';
import {
  toCurrentUrn,
  toBaselineUrn,
  isInverse,
  toColorDirection,
  makeSortable,
  appendFilters,
  hasPrefix
} from 'thirdeye-frontend/utils/rca-utils';
import {
  humanizeChange,
  humanizeFloat
} from 'thirdeye-frontend/utils/utils';
import _ from 'lodash';

const ROOTCAUSE_TRUNCATION_FRACTION = 0.0001;

export default Component.extend({
  classNames: ['rootcause-metrics'],

  /**
   * Columns for dimensions table
   * @type Object[]
   */
  dimensionsTableColumns: [
    {
      template: 'custom/table-checkbox',
      className: 'metrics-table__column metrics-table__column--checkbox'
    }, {
      propertyName: 'label',
      title: 'Dimension',
      className: 'metrics-table__column metrics-table__column--large'
    }, {
      propertyName: 'current',
      sortedBy: 'sortable_current',
      title: 'current',
      disableFiltering: true,
      className: 'metrics-table__column metrics-table__column--small'
    }, {
      propertyName: 'baseline',
      sortedBy: 'sortable_baseline',
      title: 'baseline',
      disableFiltering: true,
      className: 'metrics-table__column metrics-table__column--small'
    }, {
      template: 'custom/dimensions-table-change',
      propertyName: 'change',
      sortedBy: 'sortable_change',
      title: 'Percentage Change',
      disableFiltering: true,
      className: 'metrics-table__column metrics-table__column--small'
    }, {
      template: 'custom/dimensions-table-change',
      propertyName: 'changeContribution',
      sortedBy: 'sortable_changeContribution',
      title: 'Change in Contribution',
      disableFiltering: true,
      className: 'metrics-table__column metrics-table__column--small'
    }, {
      template: 'custom/dimensions-table-change',
      propertyName: 'contributionToChange',
      sortedBy: 'sortable_contributionToChange',
      title: 'Contribution to Change',
      disableFiltering: true,
      className: 'metrics-table__column metrics-table__column--small'
    }
  ],

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
   * Data for metrics table
   * @type Object[] - array of objects, each corresponding to a row in the table
   */
  dimensionsTableData: computed(
    'selectedUrns',
    'entities',
    'breakdowns',
    'metricUrn',
    function() {
      const { selectedUrns, entities, breakdowns, metricUrn } =
        this.getProperties('selectedUrns', 'entities', 'breakdowns', 'metricUrn');

      const current = breakdowns[toCurrentUrn(metricUrn)];
      const baseline = breakdowns[toBaselineUrn(metricUrn)];

      if (_.isEmpty(current) || _.isEmpty(baseline)) { return {}; }

      const rows = [];

      const contribTransform = (v) => Math.round(v * 10000) / 100.0;

      Object.keys(current).map(name => {
        const currTotal = this._sum(current, name);
        const baseTotal = this._sum(baseline, name);

        Object.keys(current[name]).map(value => {
          const urn = appendFilters(metricUrn, [[name, value]]);
          const curr = current[name][value] || 0;
          const base = baseline[name][value] || 0;

          if (curr === 0 && base === 0) { return; }

          if (curr / currTotal < ROOTCAUSE_TRUNCATION_FRACTION
            && base / baseTotal < ROOTCAUSE_TRUNCATION_FRACTION) { return; }

          const change = curr / base - 1;
          const changeContribution = curr / currTotal - base / baseTotal;
          const contributionToChange =  (curr - base) / baseTotal;

          // TODO support inverse metric color

          // TODO disable changeContribution and contributionToChange on non-additive metric

          rows.pushObject({
            urn,
            isSelected: selectedUrns.has(urn),
            label: `${value} (${name})`,
            current: humanizeFloat(curr),
            baseline: humanizeFloat(base),
            change: this._makeRecord(change, humanizeChange),
            changeContribution: this._makeRecord(changeContribution, contribTransform),
            contributionToChange: this._makeRecord(contributionToChange, contribTransform),
            sortable_current: makeSortable(curr),
            sortable_baseline: makeSortable(base),
            sortable_change: makeSortable(change),
            sortable_changeContribution: makeSortable(changeContribution),
            sortable_contributionToChange: makeSortable(contributionToChange)
          });
        });
      });

      return _.sortBy(rows, (row) => -1 * Math.abs(row.changeContribution));
    }
  ),

  /**
   * Sums all values for a given dimension name
   *
   * @param breakdown dimension contribution breakdown
   * @param name dimension name
   * @returns {float}
   */
  _sum(breakdown, name) {
    return Object.values(breakdown[name]).reduce((sum, v) => sum + v, 0);
  },

  /**
   * Generates template records for change-related columns.
   *
   * @param {float} change
   * @param {function} transform
   * @returns {object}
   * @private
   */
  _makeRecord(change, transform) {
    return {
      change: transform(change),
      direction: toColorDirection(change)
    };
  },

  /**
   * Keeps track of items that are selected in the table
   * @type {Array}
   */
  preselectedItems: computed(
    'metricsTableData',
    'selectedUrns',
    function () {
      return []; // FIXME: this is broken across all of RCA and works by accident only
    }
  ),

  actions: {
    /**
     * Triggered on cell selection
     * Updates the currently selected urns based on user selection on the table
     * @param {Object} e
     */
    displayDataChanged (e) {
      if (_.isEmpty(e.selectedItems)) { return; }

      const { selectedUrns, onSelection } = this.getProperties('selectedUrns', 'onSelection');

      if (!onSelection) { return; }

      const urn = e.selectedItems[0].urn;
      const state = !selectedUrns.has(urn);

      const updates = {[urn]: state};
      if (hasPrefix(urn, 'thirdeye:metric:')) {
        updates[toCurrentUrn(urn)] = state;
        updates[toBaselineUrn(urn)] = state;
      }

      onSelection(updates);
    }
  }
});

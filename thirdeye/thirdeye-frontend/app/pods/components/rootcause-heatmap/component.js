import Ember from 'ember';
import { toCurrentUrn, toBaselineUrn, filterPrefix } from 'thirdeye-frontend/helpers/utils';

export default Ember.Component.extend({
  breakdowns: null, // {}

  selectedUrns: null, // Set

  currentUrn: null, // ""

  rollup: 20, // ""

  mode: "change", // ""

  /**
   * Action to be passed into component
   * @type {Function}
   */
  onHeatmapClick: () => {},

  urns: Ember.computed(
    'selectedUrns',
    function () {
      const { selectedUrns } = this.getProperties('selectedUrns');
      return filterPrefix(selectedUrns, 'thirdeye:metric:');
    }
  ),

  current: Ember.computed(
    'breakdowns',
    'currentUrn',
    'currentUrns',
    function () {
      const { breakdowns, currentUrn, currentUrns } =
        this.getProperties('breakdowns', 'currentUrn', 'currentUrns');

      console.log('rootcauseHeatmap: current: breakdowns currentUrn', breakdowns, currentUrn);
      if (!currentUrn) {
        return {};
      }
      const breakdown = breakdowns[toCurrentUrn(currentUrn)];

      if (!breakdown) {
        return {};
      }
      return breakdown;
    }
  ),

  baseline: Ember.computed(
    'breakdowns',
    'currentUrn',
    function () {
      const { breakdowns, currentUrn } =
        this.getProperties('breakdowns', 'currentUrn');

      console.log('rootcauseHeatmap: baseline: breakdowns currentUrn', breakdowns, currentUrn);
      if (!currentUrn) {
        return {};
      }
      const breakdown = breakdowns[toBaselineUrn(currentUrn)];

      if (!breakdown) {
        return {};
      }
      return breakdown;
    }
  ),

  _dataRollup: Ember.computed(
    'current',
    'baseline',
    'rollup',
    function () {
      const { current, baseline, rollup } =
        this.getProperties('current', 'baseline', 'rollup');

        // collect all dimension names
        const dimNames = new Set(Object.keys(current).concat(Object.keys(baseline)));

        // collect all dimension values for all dimension names
        const dimValues = {};
        [...dimNames].forEach(n => dimValues[n] = new Set());
        [...dimNames].filter(n => n in current).forEach(n => Object.keys(current[n]).forEach(v => dimValues[n].add(v)));
        [...dimNames].filter(n => n in baseline).forEach(n => Object.keys(baseline[n]).forEach(v => dimValues[n].add(v)));

        const values = {};
        [...dimNames].forEach(n => {
          let curr = current[n] || {};
        let base = baseline[n] || {};
        let dimVals = dimValues[n] || new Set();

        // conditional rollup
        if (rollup > 0 && Object.keys(curr).length >= rollup) {
          const topk = new Set(this._makeTopK(curr, rollup - 1));
          curr = this._makeRollup(curr, topk, 'OTHER');
          base = this._makeRollup(base, topk, 'OTHER');
          dimVals = new Set(['OTHER', ...topk]);
        }

        values[n] = {};
        [...dimVals].forEach(v => {
          values[n][v] = {
            current: curr[v] || 0,
            baseline: base[v] || 0
          };
        });
      });

      return values;
    }
  ),

  cells: Ember.computed(
    '_dataRollup',
    'mode',
    function () {
      const { _dataRollup: data, mode } = this.getProperties('_dataRollup', 'mode');

      const transformation = this._makeTransformation(mode);

      const cells = {};
      Object.keys(data).forEach(n => {
        cells[n] = {};
        Object.keys(data[n]).forEach(v => {
          const curr = data[n][v].current;
          const base = data[n][v].baseline;
          const currTotal = this._makeSum(data[n], (d) => d.current);
          const baseTotal = this._makeSum(data[n], (d) => d.baseline);

          cells[n][v] = {
            value: Math.round(transformation(curr, base, currTotal, baseTotal) * 10000) / 100.0, // percent, 2 commas
            size: Math.round(1.0 * curr / currTotal * 10000) / 100.0 // percent, 2 commas
          }
        });
      });

      return cells;
    }
  ),

  _makeTransformation(mode) {
    switch (mode) {
      case 'change':
        return (curr, base, currTotal, baseTotal) => curr / base - 1;
      case 'contributionDiff':
        return (curr, base, currTotal, baseTotal) => curr / currTotal - base / baseTotal;
      case 'contributionToDiff':
        return (curr, base, currTotal, baseTotal) => (curr - base) / baseTotal;
    }
    return (curr, base, currTotal, baseTotal) => 0;
  },

  _makeRollup(dimNameObj, topk, otherValue) {
    if (!dimNameObj) {
      return dimNameObj;
    }
    const rollup = {};
    [...topk].forEach(v => rollup[v] = dimNameObj[v]);

    const sumOther = this._makeSumOther(dimNameObj, topk);
    rollup[otherValue] = sumOther;

    return rollup;
  },

  _makeSum(dimNameObj, funcExtract) {
    if (!dimNameObj) {
      return 0;
    }
    return Object.values(dimNameObj).reduce((agg, x) => agg + funcExtract(x), 0);
  },

  _makeSumOther(dimNameObj, topk) {
    if (!dimNameObj) {
      return 0;
    }
    return Object.keys(dimNameObj).filter(v => !topk.has(v)).map(v => dimNameObj[v]).reduce((agg, x) => agg + x, 0);
  },

  _makeTopK(dimNameObj, k) {
    if (!dimNameObj) {
      return [];
    }
    const tuples = Object.keys(dimNameObj).map(v => [-dimNameObj[v], v]).sort();
    const dimValues = _.slice(tuples, 0, k).map(t => t[1]);
    return dimValues;
  },

  actions: {
    /**
     * Bubbles the action to the parent
     */
    onHeatmapClick() {
      this.attrs.onHeatmapClick(...arguments);
    }
  }
});

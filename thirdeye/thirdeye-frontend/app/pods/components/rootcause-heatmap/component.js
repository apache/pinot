import Ember from 'ember';
import { toCurrentUrn, toBaselineUrn, filterPrefix } from 'thirdeye-frontend/helpers/utils';
import _ from 'lodash';

const ROOTCAUSE_ROLLUP_HEAD = 'HEAD';
const ROOTCAUSE_ROLLUP_TAIL = 'TAIL';

const ROOTCAUSE_ROLE_VALUE = 'value';
const ROOTCAUSE_ROLE_HEAD = 'head';
const ROOTCAUSE_ROLE_TAIL = 'tail';

export default Ember.Component.extend({
  breakdowns: null, // {}

  selectedUrns: null, // Set

  currentUrn: null, // ""

  rollupRange: [0, 20], // ""

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
    function () {
      const { breakdowns, currentUrn } =
        this.getProperties('breakdowns', 'currentUrn');

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
    'rollupRange',
    function () {
      const { current, baseline, rollupRange } =
        this.getProperties('current', 'baseline', 'rollupRange');

      // collect all dimension names
      const dimNames = new Set(Object.keys(current).concat(Object.keys(baseline)));

      // collect all dimension values for all dimension names
      const dimValues = {};
      [...dimNames].forEach(n => dimValues[n] = new Set());
      [...dimNames].filter(n => n in current).forEach(n => Object.keys(current[n]).forEach(v => dimValues[n].add(v)));
      [...dimNames].filter(n => n in baseline).forEach(n => Object.keys(baseline[n]).forEach(v => dimValues[n].add(v)));

      const values = {};
      [...dimNames].forEach(n => {
        // order based on current contribution
        const all = this._sortKeysByValue(current[n]).reverse();

        const head = _.slice(all, 0, rollupRange[0]);
        const visible = _.slice(all, rollupRange[0], rollupRange[1]);
        const tail = _.slice(all, rollupRange[1]);

        const curr = this._makeRollup(current[n], head, visible, tail);
        const base = this._makeRollup(baseline[n], head, visible, tail);

        console.log('rootcauseHeatmap: _dataRollup: n curr', n, curr);
        console.log('rootcauseHeatmap: _dataRollup: n base', n, base);

        values[n] = [ROOTCAUSE_ROLLUP_HEAD, ...visible, ROOTCAUSE_ROLLUP_TAIL].map(v => {
          return {
            role: this._makeRole(v),
            label: v,
            dimName: n,
            dimValue: v,
            current: curr[v],
            baseline: base[v]
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
      const { _dataRollup: values, mode } = this.getProperties('_dataRollup', 'mode');

      const transformation = this._makeTransformation(mode);

      console.log('rootcauseHeatmap: cells: values', values);

      const cells = {};
      Object.keys(values).forEach(n => {
        cells[n] = [];
        const valid = values[n].filter(val => val.current);

        const visibleTotal = valid.filter(v => v.role == ROOTCAUSE_ROLE_VALUE);
        const currTotal = this._makeSum(visibleTotal, (v) => v.current);
        const baseTotal = this._makeSum(visibleTotal, (v) => v.baseline);

        const sizeCoeff = 1.0 - (valid.length - visibleTotal.length) / 2.0 * 0.15; // head & tail

        valid.forEach((val, index) => {
          const curr = val.current;
          const base = val.baseline;

          let value = transformation(curr, base, currTotal, baseTotal);
          let valueLabel = `${val.label} [${index}] (${Math.round(value * 10000.0) / 100.0})`;
          if (val.role != ROOTCAUSE_ROLE_VALUE) {
            value = 0;
            valueLabel = val.label;
            if (val.label == ROOTCAUSE_ROLLUP_HEAD) {
              valueLabel = `<< ${valueLabel}`;
            } else if (val.label == ROOTCAUSE_ROLLUP_TAIL){
              valueLabel = `${valueLabel} >>`;
            }
          }

          let size = curr / currTotal * sizeCoeff;
          if (val.role != ROOTCAUSE_ROLE_VALUE) {
            size = 0.075; // head or tail
          }

          cells[n].push({
            index,
            role: val.role,
            dimName: val.dimName,
            dimValue: val.dimValue,
            label: valueLabel,
            value: value, // percent, 2 commas
            size: size // percent, 2 commas
          });
        });
      });

      return cells;
    }
  ),

  _makeRole(v) {
    switch(v) {
      case ROOTCAUSE_ROLLUP_HEAD:
        return ROOTCAUSE_ROLE_HEAD;
      case ROOTCAUSE_ROLLUP_TAIL:
        return ROOTCAUSE_ROLE_TAIL;
    }
    return ROOTCAUSE_ROLE_VALUE;
  },

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

  _makeRollup(dimNameObj, head, visible, tail) {
    if (!dimNameObj) {
      return dimNameObj;
    }

    const rollup = {};
    rollup[ROOTCAUSE_ROLLUP_HEAD] = 0;
    rollup[ROOTCAUSE_ROLLUP_TAIL] = 0;

    [...head].forEach(v => rollup[ROOTCAUSE_ROLLUP_HEAD] += dimNameObj[v] || 0);
    [...visible].forEach(v => rollup[v] = dimNameObj[v] || 0);
    [...tail].forEach(v => rollup[ROOTCAUSE_ROLLUP_TAIL] += dimNameObj[v] || 0);

    return rollup;
  },

  _makeSum(dimNameObj, funcExtract) {
    if (!dimNameObj) {
      return 0;
    }
    return Object.values(dimNameObj).reduce((agg, x) => agg + funcExtract(x), 0);
  },

  // _makeSumOther(dimNameObj, topk) {
  //   if (!dimNameObj) {
  //     return 0;
  //   }
  //   return Object.keys(dimNameObj).filter(v => !topk.has(v)).map(v => dimNameObj[v]).reduce((agg, x) => agg + x, 0);
  // },
  //
  // _makeTopK(dimNameObj, k) {
  //   if (!dimNameObj) {
  //     return [];
  //   }
  //   const tuples = Object.keys(dimNameObj).map(v => [-dimNameObj[v], v]).sort();
  //   const dimValues = _.slice(tuples, 0, k).map(t => t[1]);
  //   return dimValues;
  // },

  _sortKeysByValue(dimNameObj) {
    return Object.keys(dimNameObj).map(v => [dimNameObj[v], v]).sort((a, b) => parseFloat(a[0]) - parseFloat(b[0])).map(t => t[1]);
  },

  actions: {
    /**
     * Bubbles the action to the parent
     */
    onHeatmapClick() {
      this.attrs.onHeatmapClick(...arguments);
    },

    onRollupRange(from, to) {
      this.set('rollupRange', [parseInt(from), parseInt(to)]);
    }
  }
});

import Ember from 'ember';
import _ from 'lodash';

export default Ember.Component.extend({
  current: { // dimensions
    country: { // dimension namespace (dimNameObj)
      us: 100, // dimension value
      cn: 100,
      ca: 150
    },
    browser: {
      chrome:  250,
      firefox: 100
    }
  },

  baseline: {
    country: {
      us: 90,
      cn: 90,
      ca: 70
    },
    browser: {
      chrome:  180,
      firefox: 70
    }
  },

  mode: null, // 'change', 'contributionDiff', 'contributionToDiff'

  rollup: 10,

  init() {
    this._super(...arguments);
    this.set('mode', 'change');
  },

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

  scores: Ember.computed(
    '_dataRollup',
    'mode',
    function () {
      const { _dataRollup: data, mode } = this.getProperties('_dataRollup', 'mode');

      const transformation = this._makeTransformation(mode);

      const scores = {};
      Object.keys(data).forEach(n => {
        scores[n] = {};
        Object.keys(data[n]).forEach(v => {
          const curr = data[n][v].current;
          const base = data[n][v].baseline;
          const currTotal = this._makeSum(data[n], (d) => d.current);
          const baseTotal = this._makeSum(data[n], (d) => d.baseline);

          scores[n][v] = Math.round(transformation(curr, base, currTotal, baseTotal) * 10000) / 100.0; // percent, 2 commas
        });
      });

      return scores;
    }
  ),

  contributions: Ember.computed(
    '_dataRollup',
    function () {
      const { _dataRollup: data } = this.getProperties('_dataRollup');

      const contributions = {};
      Object.keys(data).forEach(n => {
        contributions[n] = {};
        Object.keys(data[n]).forEach(v => {
          const curr = data[n][v].current;
          const currTotal = this._makeSum(data[n], (d) => d.current);

          contributions[n][v] = Math.round(1.0 * curr / currTotal * 10000) / 100.0; // percent, 2 commas
        });
      });

      return contributions;
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
  }
});

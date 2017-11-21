import Ember from 'ember';
import { toCurrentUrn, toBaselineUrn, filterPrefix, toMetricLabel, appendTail } from 'thirdeye-frontend/helpers/utils';
import _ from 'lodash';

const ROOTCAUSE_ROLLUP_MODE_CHANGE = 'change';
const ROOTCAUSE_ROLLUP_MODE_CONTRIBUTION_DIFF = 'contributionDiff';
const ROOTCAUSE_ROLLUP_MODE_CONTRIBUTION_TO_DIFF = 'contributionToDiff';

const ROOTCAUSE_ROLLUP_HEAD = 'HEAD';
const ROOTCAUSE_ROLLUP_TAIL = 'TAIL';

const ROOTCAUSE_ROLE_VALUE = 'value';
const ROOTCAUSE_ROLE_HEAD = 'head';
const ROOTCAUSE_ROLE_TAIL = 'tail';

const ROOTCAUSE_ROLLUP_RANGE_DEFAULT = [0, 10];

export default Ember.Component.extend({
  entities: null, // {}

  breakdowns: null, // {}

  selectedUrns: null, // Set

  onSelection: null, // func (updates)

  currentUrn: null, // ""

  rollupRange: null, // ""

  mode: null, // ""

  init() {
    this._super(...arguments);

    this.setProperties({ rollupRange: {}, mode: ROOTCAUSE_ROLLUP_MODE_CHANGE});
  },

  labels: Ember.computed(
    'selectedUrns',
    'entities',
    function () {
      const { selectedUrns, entities } = this.getProperties('selectedUrns', 'entities');
      return filterPrefix(selectedUrns, 'thirdeye:metric:').reduce((agg, urn) => {
        agg[urn] = toMetricLabel(urn, entities);
        return agg;
      }, {});
    }
  ),

  current: Ember.computed(
    'breakdowns',
    'currentUrn',
    function () {
      const { breakdowns, currentUrn } =
        this.getProperties('breakdowns', 'currentUrn');

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

        const range = rollupRange[n] || ROOTCAUSE_ROLLUP_RANGE_DEFAULT;
        const head = _.slice(all, 0, range[0]);
        const visible = _.slice(all, range[0], range[1]);
        const tail = _.slice(all, range[1]);

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

        const sizeCoeff = 1.0 - (valid.length - visibleTotal.length) / 2.0 * 0.20; // head & tail

        valid.forEach((val, index) => {
          const curr = val.current;
          const base = val.baseline;

          const [value, valueLabel] = this._makeValueLabel(val, transformation(curr, base, currTotal, baseTotal));

          let size = curr / currTotal * sizeCoeff;
          if (val.role != ROOTCAUSE_ROLE_VALUE) {
            size = 0.10; // head or tail
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

  _makeValueLabel(val, value) {
    if (val.role != ROOTCAUSE_ROLE_VALUE) {
      if (val.label == ROOTCAUSE_ROLLUP_HEAD) {
        return [0, '<<'];
      }
      if (val.label == ROOTCAUSE_ROLLUP_TAIL) {
        return [0, '>>'];
      }
      return [0, val.label];
    }
    return [value, `${val.label} (${Math.round(value * 10000.0) / 100.0})`];
  },

  _makeTransformation(mode) {
    switch (mode) {
      case ROOTCAUSE_ROLLUP_MODE_CHANGE:
        return (curr, base, currTotal, baseTotal) => curr / base - 1;
      case ROOTCAUSE_ROLLUP_MODE_CONTRIBUTION_DIFF:
        return (curr, base, currTotal, baseTotal) => curr / currTotal - base / baseTotal;
      case ROOTCAUSE_ROLLUP_MODE_CONTRIBUTION_TO_DIFF:
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

  _sortKeysByValue(dimNameObj) {
    return Object.keys(dimNameObj).map(v => [dimNameObj[v], v]).sort((a, b) => parseFloat(a[0]) - parseFloat(b[0])).map(t => t[1]);
  },

  actions: {
    onHeatmapClick(role, dimName, dimValue) {
      const { rollupRange, selectedUrns, onSelection, currentUrn } =
        this.getProperties('rollupRange', 'selectedUrns', 'onSelection', 'currentUrn');

      // scrolling
      const range = rollupRange[dimName] || ROOTCAUSE_ROLLUP_RANGE_DEFAULT;
      if (role == ROOTCAUSE_ROLE_HEAD) {
        rollupRange[dimName] = range.map(v => v - 10);
        this.set('rollupRange', Object.assign({}, rollupRange));
      }
      if (role == ROOTCAUSE_ROLE_TAIL) {
        rollupRange[dimName] = range.map(v => v + 10);
        this.set('rollupRange', Object.assign({}, rollupRange));
      }

      // selection
      if (role == ROOTCAUSE_ROLE_VALUE) {
        const metricUrn = appendTail(currentUrn, `${dimName}=${dimValue}`);
        const state = !selectedUrns.has(metricUrn);
        const updates = { [metricUrn]: state, [toBaselineUrn(metricUrn)]: state, [toCurrentUrn(metricUrn)]: state };
        if (onSelection) {
          onSelection(updates);
        }
      }
    },

    onRollupRange(from, to) {
      this.set('rollupRange', [parseInt(from), parseInt(to)]);
    }
  }
});

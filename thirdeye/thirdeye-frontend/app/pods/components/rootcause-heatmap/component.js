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

const ROOTCAUSE_ROLLUP_RANGE = [0, 20];

export default Ember.Component.extend({
  //
  // external
  //
  entities: null, // {}

  breakdowns: null, // {}

  selectedUrn: null, // Set

  onSelection: null, // func (updates)

  //
  // internal
  //
  mode: null, // ""

  init() {
    this._super(...arguments);

    this.setProperties({ mode: ROOTCAUSE_ROLLUP_MODE_CHANGE });
  },

  current: Ember.computed(
    'breakdowns',
    'selectedUrn',
    function () {
      const { breakdowns, selectedUrn } =
        this.getProperties('breakdowns', 'selectedUrn');

      if (!selectedUrn) {
        return {};
      }
      const breakdown = breakdowns[toCurrentUrn(selectedUrn)];

      if (!breakdown) {
        return {};
      }
      return breakdown;
    }
  ),

  baseline: Ember.computed(
    'breakdowns',
    'selectedUrn',
    function () {
      const { breakdowns, selectedUrn } =
        this.getProperties('breakdowns', 'selectedUrn');

      if (!selectedUrn) {
        return {};
      }
      const breakdown = breakdowns[toBaselineUrn(selectedUrn)];

      if (!breakdown) {
        return {};
      }
      return breakdown;
    }
  ),

  hasCurrent: Ember.computed(
    'current',
    function () {
      return Object.keys(this.get('current')).length > 0;
    }
  ),

  hasBaseline: Ember.computed(
    'baseline',
    function () {
      return Object.keys(this.get('baseline')).length > 0;
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

        const range = ROOTCAUSE_ROLLUP_RANGE;
        const head = _.slice(all, 0, range[0]);
        const visible = _.slice(all, range[0], range[1]);
        const tail = _.slice(all, range[1]);

        const curr = this._makeRollup(current[n], head, visible, tail);
        const base = this._makeRollup(baseline[n], head, visible, tail);

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

      const cells = {};
      Object.keys(values).forEach(n => {
        cells[n] = [];
        const valid = values[n].filter(val => val.current);

        const currTotal = this._makeSum(valid, (v) => v.current);
        const baseTotal = this._makeSum(valid, (v) => v.baseline);

        valid.forEach((val, index) => {
          const curr = val.current;
          const base = val.baseline;

          const [value, valueLabel] = this._makeValueLabel(val, transformation(curr, base, currTotal, baseTotal));
          const size = curr / currTotal;

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
        return [0, 'HEAD'];
      }
      if (val.label == ROOTCAUSE_ROLLUP_TAIL) {
        return [0, 'OTHER'];
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
      return {};
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
    if (!dimNameObj) {
      return [];
    }
    return Object.keys(dimNameObj).map(v => [dimNameObj[v], v]).sort((a, b) => parseFloat(a[0]) - parseFloat(b[0])).map(t => t[1]);
  },

  actions: {
    onHeatmapClick(role, dimName, dimValue) {
      const { rollupRange, selectedUrn, onSelection, currentUrn } =
        this.getProperties('rollupRange', 'selectedUrn', 'onSelection', 'currentUrn');

      // selection
      if (role === ROOTCAUSE_ROLE_VALUE) {
        const metricUrn = appendTail(selectedUrn, `${dimName}=${dimValue}`);
        const updates = { [metricUrn]: true, [toBaselineUrn(metricUrn)]: true, [toCurrentUrn(metricUrn)]: true };
        if (onSelection) {
          onSelection(updates);
        }
      }
    }
  }
});

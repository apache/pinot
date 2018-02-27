import Component from '@ember/component';
import { getProperties, computed } from '@ember/object';
import {
  toCurrentUrn,
  toBaselineUrn,
  appendFilters,
  isInverse,
  isAdditive
} from 'thirdeye-frontend/utils/rca-utils';
import { humanizeChange } from 'thirdeye-frontend/utils/utils';
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

const ROOTCAUSE_MODE_MAPPING = {
  'Percentage Change': ROOTCAUSE_ROLLUP_MODE_CHANGE,
  'Change in Contribution': ROOTCAUSE_ROLLUP_MODE_CONTRIBUTION_DIFF,
  'Contribution to Overall Change': ROOTCAUSE_ROLLUP_MODE_CONTRIBUTION_TO_DIFF
};

export default Component.extend({
  //
  // external
  //
  entities: null, // {}

  breakdowns: null, // {}

  selectedUrn: null, // Set

  onSelection: null, // func (updates)

  isLoadingBreakdowns: null, // bool

  //
  // internal
  //

  /**
   * Selected heatmap mode
   */
  selectedMode: Object.keys(ROOTCAUSE_MODE_MAPPING)[1],

  /**
   * Lists out all heatmap mode options
   */
  modeOptions: Object.keys(ROOTCAUSE_MODE_MAPPING),

  /**
   * Mapping of human readable to mode option
   */
  modeMapping: ROOTCAUSE_MODE_MAPPING,

  /**
   * Computes the correct mode option based on the
   * selected human readable option
   * @type {String}
   */
  mode: computed('selectedMode', function() {
    const {
      selectedMode,
      modeMapping
    } = getProperties(this, 'selectedMode', 'modeMapping');

    return modeMapping[selectedMode];
  }),

  current: computed(
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

  baseline: computed(
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

  hasCurrent: computed(
    'current',
    'isLoadingBreakdowns',
    function () {
      const { current, isLoadingBreakdowns } = this.getProperties('current', 'isLoadingBreakdowns');
      return isLoadingBreakdowns || !_.isEmpty(current);
    }
  ),

  hasBaseline: computed(
    'baseline',
    'isLoadingBreakdowns',
    function () {
      const { baseline, isLoadingBreakdowns } = this.getProperties('baseline', 'isLoadingBreakdowns');
      return isLoadingBreakdowns || !_.isEmpty(baseline);
    }
  ),

  isInverse: computed(
    'selectedUrn',
    'entities',
    function () {
      const { selectedUrn, entities } = this.getProperties('selectedUrn', 'entities');
      return isInverse(selectedUrn, entities);
    }
  ),

  isAdditive: computed(
    'selectedUrn',
    'entities',
    function () {
      const { selectedUrn, entities } = this.getProperties('selectedUrn', 'entities');
      // prevent flashing error message
      if (!(selectedUrn in entities)) {
        return true;
      }

      return isAdditive(selectedUrn, entities);
    }
  ),

  _dataRollup: computed(
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

  cells: computed(
    '_dataRollup',
    'mode',
    'isInverse',
    function () {
      const { _dataRollup: values, mode, isInverse } =
        this.getProperties('_dataRollup', 'mode', 'isInverse');

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

          const labelChange = transformation(curr, base, currTotal, baseTotal);

          const [value, labelValue] = this._makeValueLabel(val, labelChange);
          const size = curr / currTotal;

          cells[n].push({
            index,
            role: val.role,
            dimName: val.dimName,
            dimValue: val.dimValue,
            label: labelValue,
            value,
            size,
            inverse: isInverse,

            currTotal,
            baseTotal,
            changeTotal: humanizeChange(currTotal / baseTotal - 1),

            curr,
            base,
            change: humanizeChange(curr / base - 1),

            currContrib: `${(Math.round(curr / currTotal * 1000) / 10.0).toFixed(1)}%`,
            baseContrib: `${(Math.round(base / baseTotal * 1000) / 10.0).toFixed(1)}%`,
            changeContrib: humanizeChange((curr / currTotal) - (base / baseTotal))
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
    return (curr, base, currTotal, baseTotal) => parseFloat('NaN');
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
        const metricUrn = appendFilters(selectedUrn, [[dimName, dimValue]]);
        const updates = { [metricUrn]: true, [toBaselineUrn(metricUrn)]: true, [toCurrentUrn(metricUrn)]: true };
        if (onSelection) {
          onSelection(updates);
        }
      }
    }
  }
});

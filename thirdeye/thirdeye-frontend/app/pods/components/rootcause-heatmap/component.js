import Component from '@ember/component';
import { get, getProperties, computed } from '@ember/object';
import {
  toCurrentUrn,
  toBaselineUrn,
  appendFilters,
  isInverse,
  isAdditive,
  filterPrefix,
  toMetricLabel
} from 'thirdeye-frontend/utils/rca-utils';
import { humanizeChange, humanizeFloat } from 'thirdeye-frontend/utils/utils';
import _ from 'lodash';

const ROOTCAUSE_ROLLUP_MODE_CHANGE = 'change';
const ROOTCAUSE_ROLLUP_MODE_CONTRIBUTION_DIFF = 'contributionDiff';
const ROOTCAUSE_ROLLUP_MODE_CONTRIBUTION_TO_DIFF = 'contributionToDiff';

const ROOTCAUSE_ROLLUP_HEAD = 'HEAD';
const ROOTCAUSE_ROLLUP_TAIL = 'TAIL';

const ROOTCAUSE_ROLE_VALUE = 'value';
const ROOTCAUSE_ROLE_HEAD = 'head';
const ROOTCAUSE_ROLE_TAIL = 'tail';

const ROOTCAUSE_ROLLUP_RANGE = [0, 100];
const ROOTCAUSE_ROLLUP_EXPLAIN_FRACTION = 0.95;

const ROOTCAUSE_VALUE_OTHER = 'OTHER';

const ROOTCAUSE_MODE_MAPPING = {
  'Percentage Change': ROOTCAUSE_ROLLUP_MODE_CHANGE,
  'Change in Contribution': ROOTCAUSE_ROLLUP_MODE_CONTRIBUTION_DIFF,
  'Contribution to Overall Change': ROOTCAUSE_ROLLUP_MODE_CONTRIBUTION_TO_DIFF
};

export default Component.extend({
  //
  // external
  //

  /**
   * RCA entities
   * @type {object}
   */
  entities: null,

  /**
   * Breakdowns
   * @type {object}
   */
  breakdowns: null,

  /**
   * Selected metric urn
   * @type String
   */
  selectedUrn: null,

  /**
   * Call back function for clicking heatmap
   * @type {function}
   */
  onSelection: null,

  /**
   * Call back function for size metric selection
   * @type {function}
   */
  onSizeMetric: null,

  /**
   * The urn for
   * @type String
   */
  sizeMetricUrn: null,

  isLoadingBreakdowns: null,

  //
  // internal
  //

  /**
   * Selected heatmap mode
   */
  // eslint-disable-next-line ember/avoid-leaking-state-in-ember-objects
  selectedMode: Object.keys(ROOTCAUSE_MODE_MAPPING)[1],

  /**
   * Selected option for heatmap size
   */
  sizeOptionSelected: computed('sizeMetricUrn', 'sizeOptions', function () {
    const { sizeMetricUrn, sizeOptions } = getProperties(this, 'sizeMetricUrn', 'sizeOptions');
    return sizeOptions.find((opt) => opt.urn === sizeMetricUrn);
  }).readOnly(),

  /**
   * Options for heatmap size
   */
  sizeOptions: computed('entities', function () {
    const entities = get(this, 'entities');
    const metricUrns = filterPrefix(Object.keys(entities), 'thirdeye:metric:');
    return _.sortBy(
      metricUrns.map((urn) => Object.assign({}, { urn, name: toMetricLabel(urn, entities) })),
      'name'
    );
  }),

  /**
   * Lists out all heatmap mode options
   */
  // eslint-disable-next-line ember/avoid-leaking-state-in-ember-objects
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
  mode: computed('selectedMode', function () {
    const { selectedMode, modeMapping } = getProperties(this, 'selectedMode', 'modeMapping');

    return modeMapping[selectedMode];
  }),

  current: computed('breakdowns', 'selectedUrn', function () {
    const { breakdowns, selectedUrn } = getProperties(this, 'breakdowns', 'selectedUrn');

    if (!selectedUrn) {
      return {};
    }
    const breakdown = breakdowns[toCurrentUrn(selectedUrn)];

    if (!breakdown) {
      return {};
    }
    return breakdown;
  }),

  /**
   * breakdown for size metric
   */
  sizeMetricCurrent: computed('breakdowns', 'sizeMetricUrn', function () {
    const { breakdowns, sizeMetricUrn } = getProperties(this, 'breakdowns', 'sizeMetricUrn');
    if (!sizeMetricUrn) {
      return {};
    }
    const breakdown = breakdowns[toCurrentUrn(sizeMetricUrn)];
    if (!breakdown) {
      return {};
    }
    return breakdown;
  }),

  baseline: computed('breakdowns', 'selectedUrn', function () {
    const { breakdowns, selectedUrn } = getProperties(this, 'breakdowns', 'selectedUrn');
    if (!selectedUrn) {
      return {};
    }
    const breakdown = breakdowns[toBaselineUrn(selectedUrn)];

    if (!breakdown) {
      return {};
    }
    return breakdown;
  }),

  hasCurrent: computed('current', 'isLoadingBreakdowns', function () {
    const { current, isLoadingBreakdowns } = getProperties(this, 'current', 'isLoadingBreakdowns');
    return isLoadingBreakdowns || !_.isEmpty(current);
  }),

  hasBaseline: computed('baseline', 'isLoadingBreakdowns', function () {
    const { baseline, isLoadingBreakdowns } = getProperties(this, 'baseline', 'isLoadingBreakdowns');
    return isLoadingBreakdowns || !_.isEmpty(baseline);
  }),

  isInverse: computed('selectedUrn', 'entities', function () {
    const { selectedUrn, entities } = getProperties(this, 'selectedUrn', 'entities');
    return isInverse(selectedUrn, entities);
  }),

  isAdditive: computed('selectedUrn', 'entities', function () {
    const { selectedUrn, entities } = getProperties(this, 'selectedUrn', 'entities');
    // prevent flashing error message
    if (!(selectedUrn in entities)) {
      return true;
    }

    return isAdditive(selectedUrn, entities);
  }),

  _dataRollup: computed('current', 'baseline', 'sizeMetricCurrent', 'rollupRange', function () {
    const { current, baseline, sizeMetricCurrent } = getProperties(this, 'current', 'baseline', 'sizeMetricCurrent');

    if (!sizeMetricCurrent) {
      return;
    }
    // collect all dimension names
    const dimNames = new Set(Object.keys(current).concat(Object.keys(baseline)));

    // collect all dimension values for all dimension names
    const dimValues = {};
    [...dimNames].forEach((n) => (dimValues[n] = new Set()));
    [...dimNames]
      .filter((n) => n in current)
      .forEach((n) => Object.keys(current[n]).forEach((v) => dimValues[n].add(v)));
    [...dimNames]
      .filter((n) => n in baseline)
      .forEach((n) => Object.keys(baseline[n]).forEach((v) => dimValues[n].add(v)));

    const values = {};
    [...dimNames].forEach((n) => {
      // order based on current contribution
      const all = this._sortKeysByValue(sizeMetricCurrent[n]).reverse();

      const range = ROOTCAUSE_ROLLUP_RANGE;
      const head = _.slice(all, 0, range[0]);
      if (!(n in sizeMetricCurrent)) {
        return;
      }

      // select keys explaining ROOTCAUSE_ROLLUP_EXPLAIN_FRACTION of secondary total
      const visible = [];
      const cutoff = this._sum(all.map((v) => sizeMetricCurrent[n][v])) * ROOTCAUSE_ROLLUP_EXPLAIN_FRACTION;
      let sum = this._sum(head.map((v) => sizeMetricCurrent[n][v]));
      let i = range[0];
      while (i < range[1] && sum < cutoff && all[i] !== ROOTCAUSE_VALUE_OTHER) {
        sum += sizeMetricCurrent[n][all[i]];
        visible.push(all[i]);
        i++;
      }

      const tail = _.slice(all, i);

      const curr = this._makeRollup(current[n], head, visible, tail);
      const base = this._makeRollup(baseline[n], head, visible, tail);
      const size = this._makeRollup(sizeMetricCurrent[n], head, visible, tail);

      values[n] = [ROOTCAUSE_ROLLUP_HEAD, ...visible, ROOTCAUSE_ROLLUP_TAIL].map((v) => {
        return {
          role: this._makeRole(v),
          label: v,
          dimName: n,
          dimValue: v,
          current: curr[v],
          baseline: base[v],
          size: size[v]
        };
      });
    });

    return values;
  }),

  cells: computed('_dataRollup', 'mode', 'isInverse', function () {
    const { _dataRollup: values, mode, isInverse } = getProperties(this, '_dataRollup', 'mode', 'isInverse');
    const transformation = this._makeTransformation(mode);
    const cells = {};
    Object.keys(values).forEach((n) => {
      cells[n] = [];
      const valid = values[n].filter((val) => val.current);

      const currTotal = this._makeSum(valid, (v) => v.current);
      const baseTotal = this._makeSum(valid, (v) => v.baseline);
      const sizeTotal = this._makeSum(valid, (v) => v.size);

      valid.forEach((val, index) => {
        const curr = val.current;
        const base = val.baseline;
        const size = val.size;
        const labelChange = transformation(curr, base, currTotal, baseTotal);
        const [value, labelValue] = this._makeValueLabel(val, labelChange);
        const sizeRatioLabel = size / sizeTotal < 0.01 ? '' : labelValue;

        cells[n].push({
          index,
          role: val.role,
          dimName: val.dimName,
          dimValue: val.dimValue,
          label: sizeRatioLabel,
          value,
          size: size / sizeTotal,
          inverse: isInverse,

          currTotal: humanizeFloat(currTotal),
          baseTotal: humanizeFloat(baseTotal),
          changeTotal: humanizeChange(currTotal / baseTotal - 1),

          curr: humanizeFloat(curr),
          base: humanizeFloat(base),
          change: humanizeChange(curr / base - 1),

          currContrib: `${(Math.round((curr / currTotal) * 1000) / 10.0).toFixed(1)}%`,
          baseContrib: `${(Math.round((base / baseTotal) * 1000) / 10.0).toFixed(1)}%`,
          changeContrib: humanizeChange(curr / currTotal - base / baseTotal)
        });
      });
    });

    return cells;
  }),

  _makeRole(v) {
    switch (v) {
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
        return (curr, base /*, currTotal, baseTotal */) => curr / base - 1;
      case ROOTCAUSE_ROLLUP_MODE_CONTRIBUTION_DIFF:
        return (curr, base, currTotal, baseTotal) => curr / currTotal - base / baseTotal;
      case ROOTCAUSE_ROLLUP_MODE_CONTRIBUTION_TO_DIFF:
        return (curr, base, currTotal, baseTotal) => (curr - base) / baseTotal;
    }
    return (/*curr, base, currTotal, baseTotal */) => parseFloat('NaN');
  },

  _makeRollup(dimNameObj, head, visible, tail) {
    if (!dimNameObj) {
      return {};
    }

    const rollup = {};
    rollup[ROOTCAUSE_ROLLUP_HEAD] = 0;
    rollup[ROOTCAUSE_ROLLUP_TAIL] = 0;

    [...head].forEach((v) => (rollup[ROOTCAUSE_ROLLUP_HEAD] += dimNameObj[v] || 0));
    [...visible].forEach((v) => (rollup[v] = dimNameObj[v] || 0));
    [...tail].forEach((v) => (rollup[ROOTCAUSE_ROLLUP_TAIL] += dimNameObj[v] || 0));

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
    return Object.keys(dimNameObj)
      .map((v) => [dimNameObj[v], v])
      .sort((a, b) => {
        if (a[1] === ROOTCAUSE_VALUE_OTHER) {
          return parseFloat('inf');
        }
        if (b[1] === ROOTCAUSE_VALUE_OTHER) {
          return parseFloat('-inf');
        }
        return parseFloat(a[0]) - parseFloat(b[0]);
      })
      .map((t) => t[1]);
  },

  _sum(arr) {
    return arr.reduce((agg, x) => agg + x, 0);
  },

  actions: {
    onInclude(role, dimName, dimValue) {
      const { selectedUrn, onSelection } = getProperties(this, 'selectedUrn', 'onSelection');

      // selection
      if (role === ROOTCAUSE_ROLE_VALUE) {
        const metricUrn = appendFilters(selectedUrn, [[dimName, '=', dimValue]]);
        const updates = { [metricUrn]: true, [toBaselineUrn(metricUrn)]: true, [toCurrentUrn(metricUrn)]: true };
        if (onSelection) {
          onSelection(updates);
        }
      }
    },

    onExclude(role, dimName, dimValue) {
      const { selectedUrn, onSelection } = getProperties(this, 'selectedUrn', 'onSelection');

      // selection
      if (role === ROOTCAUSE_ROLE_VALUE) {
        const metricUrn = appendFilters(selectedUrn, [[dimName, '!=', dimValue]]);
        const updates = { [metricUrn]: true, [toBaselineUrn(metricUrn)]: true, [toCurrentUrn(metricUrn)]: true };
        if (onSelection) {
          onSelection(updates);
        }
      }
    },

    /**
     * Load size metric
     */
    onSizeMetric(option) {
      const { onSizeMetric } = getProperties(this, 'onSizeMetric');
      if (onSizeMetric) {
        onSizeMetric(option.urn);
      }
    }
  }
});

import Ember from 'ember';

export default Ember.Component.extend({
  current: //null, // {}
  {
    country: {
      us: 100,
      cn: 100,
      ca: 150
    },
    browser: {
      chrome:  250,
      firefox: 100
    }
  },

  baseline: // null, // {}
  {
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

  values: Ember.computed(
    'current',
    'baseline',
    'mode',
    function () {
      const { current, baseline, mode } = this.getProperties('current', 'baseline', 'mode');

      // collect all dimension names
      const dimNames = new Set(Object.keys(current).concat(Object.keys(baseline)));

      // collect all dimension values for all dimension names
      const dimValues = {};
      [...dimNames].forEach(n => dimValues[n] = new Set());
      [...dimNames].filter(n => n in current).forEach(n => Object.keys(current[n]).forEach(v => dimValues[n].add(v)));
      [...dimNames].filter(n => n in baseline).forEach(n => Object.keys(baseline[n]).forEach(v => dimValues[n].add(v)));

      // tranformation
      const transformation = this._makeTransformation(mode);

      const values = {};
      [...dimNames].forEach(n => {
        const currTotal = this._makeSum(current[n]);
        const baseTotal = this._makeSum(baseline[n]);
        values[n] = {};

        [...dimValues[n]].forEach(v => {
          const curr = current[n][v] || 0;
          const base = baseline[n][v] || 0;

          values[n][v] = transformation(curr, base, currTotal, baseTotal);
        });
      });

      return values;
    }
  ),

  _makeTransformation(mode) {
    switch (mode) {
      case 'change':
        return (curr, base, currTotal, baseTotal) => curr / base - 1;
      case 'contributionDiff':
        return (curr, base, currTotal, baseTotal) => curr / currTotal - base / baseTotal;
      case 'contributionToDiff':
        return (curr, base, currTotal, baseTotal) => (curr - base) / (currTotal - baseTotal);
    }
    return (curr, base, currTotal, baseTotal) => 0;
  },

  _makeSum(dimNameObj) {
    if (!dimNameObj) {
      return 0;
    }
    return Object.values(dimNameObj).reduce((agg, x) => agg + x, 0);
  },

  actions: {
    setMode(mode) {
      this.set('mode', mode);
    }
  }

});

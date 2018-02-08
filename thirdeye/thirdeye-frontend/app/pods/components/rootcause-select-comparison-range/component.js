import Ember from 'ember';
import moment from 'moment';

/**
 * Date format the date picker component expects
 * @type String
 *
 */
const serverDateFormat = 'YYYY-MM-DD HH:mm';

export default Ember.Component.extend({
  range: null, // [0, 0]

  compareMode: null, // ""

  onChange: null, // func (start, end, compareMode)

  rangeOptions: {
    'Last hour': [moment().subtract(1, 'hours').startOf('hour'), moment().startOf('hours').add(1, 'hours')],
    'Last 3 hours': [moment().subtract(3, 'hours').startOf('hour'), moment().startOf('hours').add(1, 'hours')],
    'Last 6 hours': [moment().subtract(6, 'hours').startOf('hour'), moment().startOf('hours').add(1, 'hours')],
    'Last 24 hours': [moment().subtract(24, 'hours').startOf('hour'), moment().startOf('hours').add(1, 'hours')]
  },

  compareModeOptions: [
    'WoW',
    'Wo2W',
    'Wo3W',
    'Wo4W'
  ],

  maxDateFormatted: Ember.computed({
    get() {
      return moment().startOf('hour').add(1, 'hours').format(serverDateFormat);
    }
  }),

  startFormatted: Ember.computed('range', {
    get() {
      return moment(this.get('range')[0]).format(serverDateFormat);
    }
  }),

  endFormatted: Ember.computed('range', {
    get() {
      return moment(this.get('range')[1]).format(serverDateFormat);
    }
  }),

  compareModeFormatted: Ember.computed('compareMode', {
    get() {
      return this.get('compareMode');
    }
  }),

  actions: {
    onRange(start, end) {
      const { compareMode, onChange } = this.getProperties('compareMode', 'onChange');
      onChange(moment(start).valueOf(), moment(end).valueOf(), compareMode);
    },

    onCompareMode(compareMode) {
      const { range, onChange } = this.getProperties('range', 'onChange');
      onChange(range[0], range[1], compareMode);
    }
  }
});

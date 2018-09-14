import { computed } from '@ember/object';
import Component from '@ember/component';
import moment from 'moment';
import $ from 'jquery';

/**
 * Date format the date picker component expects
 * @type String
 *
 */
const serverDateFormat = 'YYYY-MM-DD HH:mm';

/**
 * @summary Mapping between values that are named on the backend to epoc time value
 * @type {Object}
 * @example  `1_HOURS=3600=1hr , 1800=30mins` etc since we using epoc time values
 */
const namedToEpocMapping = {
  '5_MINUTES': 300,
  '15_MINUTES': 900,
  '30_MINUTES': 1800,
  '1_HOURS': 3600,
  '3_HOURS': 10800,
  '1_DAYS': 86400
};


export default Component.extend({
  timeFormat: "MMM D, hh:mm a",//slider
  range: null, // [0, 0]
  compareMode: null, // ""
  onChange: null, // func (start, end, compareMode)
  slider: null,
  originalMinInvestigatePeriod: null,
  originalMaxInvestigatePeriod: null,

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
    'Wo4W',
    'mean4w',
    'median4w',
    'min4w',
    'max4w',
    'predicted',
    'none'
  ],

  minDisplayWindow: computed('displayRange.[]', function() {//display window start - slider
    return moment(this.get('displayRange')[0]);
  }),

  maxDisplayWindow: computed('displayRange.[]', function() {//display window end - slider
    return moment(this.get('displayRange')[1]);
  }),

  minInvestigatePeriod: computed('anomalyRange.[]', function() {//Investigation period start - slider
    return moment(this.get('anomalyRange')[0]);
  }),

  maxInvestigatePeriod: computed('anomalyRange.[]', function() {//Investigation period - slider
    return moment(this.get('anomalyRange')[1]);
  }),

  granularityOneWay: computed('granularity', function() {
    return namedToEpocMapping[this.get('granularity')];
  }),


  startFormatted: computed('anomalyRange.[]', function() {//investigation start
    return moment(this.get('anomalyRange')[0]).format(serverDateFormat);
  }),

  endFormatted: computed('anomalyRange.[]', function() {//investigation end
    return moment(this.get('anomalyRange')[1]).format(serverDateFormat);
  }),

  maxDateFormatted: computed(function() {
    return moment().startOf('hour').add(1, 'hours').format(serverDateFormat);
  }),

  compareModeFormatted: computed('compareMode', function() {
    return this.get('compareMode');
  }),

  /**
   * Default after elements are inserted
   */
  didInsertElement() {
    this._super(...arguments);
    let $range = $('.js-range-slider');
    const timeFormat = this.get('timeFormat');
    const { compareMode, onChange } = this.getProperties('compareMode', 'onChange');

    $range.ionRangeSlider({
      type: 'double',
      grid: true,
      grid_num: 1,
      hide_min_max: true,
      step: this.get('granularityOneWay'),
      min: this.get('minDisplayWindow').format('X'),
      max: this.get('maxDisplayWindow').format('X'),
      from: this.get('minInvestigatePeriod').format('X'),
      to: this.get('maxInvestigatePeriod').format('X'),
      prettify: function (num) {
        return moment(num, 'X').format(timeFormat);
      },
      onFinish: function (data) {
        // Update the display window's investigation period on the chart
        onChange(moment.unix(data.from).valueOf(), moment.unix(data.to).valueOf(), compareMode);
      }
    });

    // Save slider instance to var
    this.set('slider', $range.data('ionRangeSlider'));
  },

  didRender() {
    this._super(...arguments);

    // Set oneway assignment from these existing CPs
    this.setProperties({
      startFormattedOneWay: this.get('startFormatted'),
      endFormattedOneWay: this.get('endFormatted'),
      maxDateFormattedOneWay: this.get('maxDateFormatted'),
      // granularityOneWay: this.get('granularity')
    });

    // Save original investigation periods
    this.setProperties({
      originalMinInvestigatePeriod: this.get('minInvestigatePeriod'),
      originalMaxInvestigatePeriod: this.get('maxInvestigatePeriod')
    });

    // Update the slider, by calling it's update method
    if (this.get('slider')) {
      this.get('slider').update({
        step: this.get('granularityOneWay'),
        min: this.get('minDisplayWindow').format('X'),
        max: this.get('maxDisplayWindow').format('X')
      });
    }
  },

  actions: {
    onRange(start, end) {
      const { compareMode, onChange } = this.getProperties('compareMode', 'onChange');

      // Update anomalyRange for computed to recalculate
      this.set('anomalyRange', [moment(start).valueOf(), moment(end).valueOf()]);

      // Investigation period changed on date picker. Update the slider's min and max.
      this.get('slider').update({
        from: this.get('minInvestigatePeriod').format('X'),
        to: this.get('maxInvestigatePeriod').format('X')
      });

      // Update the display window's investiation period on the chart
      onChange(moment(start).valueOf(), moment(end).valueOf(), compareMode);
    },

    onPickerRange(type, time) {
      const { compareMode, onChange } = this.getProperties('compareMode', 'onChange');

      // Update anomalyRange for computed to recalculate
      const start = type === 'start' ? moment(time).valueOf() : this.get('minInvestigatePeriod').valueOf();
      const end = type === 'start' ? this.get('maxInvestigatePeriod').valueOf() : moment(time).valueOf();

      // Update for the date picker to be in sync
      this.set('anomalyRange', [moment(start).valueOf(), moment(end).valueOf()]);

      // Investigation period changed on date picker. Update the slider's min and max.
      let sliderOptions = type === 'start' ? { from: this.get('minInvestigatePeriod').format('X') } : { to: this.get('maxInvestigatePeriod').format('X') };
      this.get('slider').update(sliderOptions);

      // Update the display window's investiation period on the chart
      onChange(moment(start).valueOf(), moment(end).valueOf(), compareMode);
    },

    onCompareMode(compareMode) {
      const { anomalyRange, onChange } = this.getProperties('anomalyRange', 'onChange');
      onChange(anomalyRange[0], anomalyRange[1], compareMode);
    },

    resetSlider() {
      const slider = this.get('slider');
      // RESET - reset slider to it's first values
      slider.reset();
      // get original investigation periods
      this.setProperties({
        minInvestigatePeriod: this.get('originalMinInvestigatePeriod'),
        maxInvestigatePeriod: this.get('originalMaxInvestigatePeriod')
      });

      // Update the slider, by calling it's update method
      this.get('slider').update({
        from: this.get('minInvestigatePeriod').format('X'),
        to: this.get('maxInvestigatePeriod').format('X')
      });
    }

  }
});

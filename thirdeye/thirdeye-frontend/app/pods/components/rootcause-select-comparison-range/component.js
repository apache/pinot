import { computed, set } from '@ember/object';
import Component from '@ember/component';
import $ from 'jquery';
import { makeTime } from 'thirdeye-frontend/utils/rca-utils';
import _ from 'lodash';

// TODO consolidate rootcause-select-comparison-range2, rootcause-slider

/**
 * Date format the date picker component expects
 * @type String
 *
 */
const serverDateFormat = 'YYYY-MM-DD HH:mm';

/**
 * @summary Mapping between values that are named on the backend to epoc time value in millis
 * @type {Object}
 * @example  `1_HOURS=3600000=1hr , 1800000=30mins` etc since we using epoc time values in millis
 */
const namedToEpocMapping = {
  '5_MINUTES': 300000,
  '15_MINUTES': 900000,
  '30_MINUTES': 1800000,
  '1_HOURS': 3600000,
  '3_HOURS': 10800000,
  '1_DAYS': 86400000
};

export default Component.extend({
  timeFormat: 'MMM D, hh:mm a z', //slider
  range: null, // [0, 0]
  compareMode: null, // ""
  onChange: null, // func (start, end, compareMode)
  slider: null,
  sliderOptionsCache: null,
  showBaselineModal: false,
  showForecastTimeRanges: false,
  customBaselineValue: 'wo1w',
  // eslint-disable-next-line ember/avoid-leaking-state-in-ember-objects
  rangeOptions: {
    'Last hour': [makeTime().subtract(1, 'hours').startOf('hour'), makeTime().startOf('hours').add(1, 'hours')],
    'Last 3 hours': [makeTime().subtract(3, 'hours').startOf('hour'), makeTime().startOf('hours').add(1, 'hours')],
    'Last 6 hours': [makeTime().subtract(6, 'hours').startOf('hour'), makeTime().startOf('hours').add(1, 'hours')],
    'Last 24 hours': [makeTime().subtract(24, 'hours').startOf('hour'), makeTime().startOf('hours').add(1, 'hours')]
  },
  // eslint-disable-next-line ember/avoid-leaking-state-in-ember-objects
  compareModeOptions: [
    {
      groupName: 'Weekly',
      options: ['wo1w', 'wo2w', 'wo3w', 'mean4w', 'median4w']
    },
    {
      groupName: 'Algorithm',
      options: ['predicted', 'forecast']
    },
    {
      groupName: 'Custom Baseline Selector',
      options: ['custom']
    }
  ],

  minDisplayWindow: computed('displayRange', function () {
    //display window start - slider
    return makeTime(this.get('displayRange')[0]);
  }),

  maxDisplayWindow: computed('displayRange', function () {
    //display window end - slider
    return makeTime(this.get('displayRange')[1]);
  }),

  minInvestigatePeriod: computed('anomalyRange', function () {
    //Investigation period start - slider
    return makeTime(this.get('anomalyRange')[0]);
  }),

  maxInvestigatePeriod: computed('anomalyRange', function () {
    //Investigation period - slider
    return makeTime(this.get('anomalyRange')[1]);
  }),

  granularityOneWay: computed('granularity', function () {
    return namedToEpocMapping[this.get('granularity')];
  }),

  startFormatted: computed('anomalyRange', function () {
    //investigation start
    return makeTime(this.get('anomalyRange')[0]).format(serverDateFormat);
  }),

  endFormatted: computed('anomalyRange', function () {
    //investigation end
    return makeTime(this.get('anomalyRange')[1]).format(serverDateFormat);
  }),

  maxDateFormatted: computed('compareMode', function () {
    const compareMode = this.get('compareMode');
    if (compareMode === 'forecast') {
      return null;
    }
    return makeTime().startOf('hour').add(1, 'hours').format(serverDateFormat);
  }),

  compareModeFormatted: computed('compareMode', {
    get() {
      return this.get('compareMode');
    },
    set() {
      // ignore to prevent override
    }
  }),

  /**
   * Default after elements are inserted
   */
  didInsertElement() {
    this._super(...arguments);
    let $range = $('.js-range-slider');
    const timeFormat = this.get('timeFormat');
    const { onChange } = this.getProperties('onChange');
    const $$ = this;

    $range.ionRangeSlider({
      type: 'double',
      grid: true,
      grid_num: 1,
      hide_min_max: true,
      step: this.get('granularityOneWay'),
      min: this.get('minDisplayWindow').format('x'),
      max: this.get('maxDisplayWindow').format('x'),
      from: this.get('minInvestigatePeriod').format('x'),
      to: this.get('maxInvestigatePeriod').format('x'),
      prettify: function (num) {
        return makeTime(num).format(timeFormat);
      },
      onFinish: function (data) {
        // Update the display window's investigation period on the chart
        onChange(makeTime(data.from).valueOf(), makeTime(data.to).valueOf(), $$.get('compareMode'));
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
      maxDateFormattedOneWay: this.get('maxDateFormatted')
      // granularityOneWay: this.get('granularity')
    });

    // Update the slider, by calling it's update method
    const sliderOptionsCache = this.get('sliderOptionsCache');

    const sliderOptions = {
      step: this.get('granularityOneWay'),
      min: this.get('minDisplayWindow').format('x'),
      max: this.get('maxDisplayWindow').format('x'),
      from: this.get('minInvestigatePeriod').format('x'),
      to: this.get('maxInvestigatePeriod').format('x')
    };

    if (!_.isEqual(sliderOptions, sliderOptionsCache)) {
      this.get('slider').update(sliderOptions);
      this.set('sliderOptionsCache', sliderOptions);
    }
  },

  actions: {
    onPickerRange(type, time) {
      const { compareMode, onChange } = this.getProperties('compareMode', 'onChange');

      // Update anomalyRange for computed to recalculate
      const start = type === 'start' ? makeTime(time).valueOf() : this.get('minInvestigatePeriod').valueOf();
      const end = type === 'start' ? this.get('maxInvestigatePeriod').valueOf() : makeTime(time).valueOf();

      // Update for the date picker to be in sync
      this.set('anomalyRange', [makeTime(start).valueOf(), makeTime(end).valueOf()]);

      // Update the display window's investiation period on the chart
      onChange(makeTime(start).valueOf(), makeTime(end).valueOf(), compareMode);
    },

    updateCustomBaseline(baseline) {
      set(this, 'customBaselineValue', baseline);
    },

    onBaseline() {
      set(this, 'showBaselineModal', false);
      this.send('onCompareMode', this.get('customBaselineValue'));
    },

    onCancel() {
      set(this, 'showBaselineModal', false);
    },

    onCompareMode(compareMode) {
      if (compareMode === 'custom') {
        set(this, 'showBaselineModal', true);
      } else {
        set(this, 'showForecastTimeRanges', compareMode === 'forecast');
        const { anomalyRange, onChange } = this.getProperties('anomalyRange', 'onChange');
        onChange(anomalyRange[0], anomalyRange[1], compareMode);
      }
    }
  }
});

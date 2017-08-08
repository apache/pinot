import Ember from 'ember';
import moment from 'moment';

const serverDateFormat = 'YYYY-MM-DD HH:mm';

const isBetween = (date, start, end) => {
  date = Number(date);
  start = Number(start);
  end = Number(end);

  return moment(date).isBetween(start, end);
};

export default Ember.Controller.extend({
  queryParams: [
    'granularity',
    'filters',
    'compareMode',
    'startDate',
    'endDate',
    'analysisStart',
    'analysisEnd'
  ],
  granularities: Ember.computed.reads('model.granularities'),
  noMatchesMessage: '',
  filters: null,
  compareMode: null,
  compareModeOptions: ['WoW', 'Wo2W', 'Wo3W', 'Wo4W'],
  mostRecentTask: null,
  metricFilters: Ember.computed.reads('model.metricFilters'),
  subchartStart: 0,
  subchartEnd: 0,
  predefinedRanges: {},

  /**
   * Upper bound (end) for date range
   * @type {String}
   */
  maxTime: Ember.computed('model.maxTime', function() {
    const maxTime = this.get('model.maxTime');

    return maxTime
      ? moment(maxTime).format(serverDateFormat)
      : moment().format(serverDateFormat);
  }),

  /**
   * Indicates the date format to be used based on granularity
   * @type {String}
   */
  uiDateFormat: Ember.computed('granularity', function() {
    const granularity = this.get('granularity');

    switch(granularity) {
      case 'DAYS':
        return 'MMM D, YYYY';
      case 'HOURS':
        return 'MMM D, YYYY h a';
      default:
        return 'MMM D, YYYY hh:mm a';
    }
  }),

  /**
   * Indicates the allowed date range picker increment based on granularity
   * @type {Number}
   */
  timePickerIncrement: Ember.computed('granularity', function() {
    const granularity = this.get('granularity');

    switch(granularity) {
      case 'DAYS':
        return 1440;
      case 'HOURS':
        return 60;
      default:
        return 5;
    }
  }),

  /**
   * Determines if the date range picker should show time selection
   * @type {Boolean}
   */
  showTimePicker: Ember.computed('granularity', function() {
    const granularity = this.get('granularity');

    return granularity !== 'DAYS';
  }),

  // converts analysisStart from unix ms to serverDateFormat
  anomalyRegionStart: Ember.computed('analysisStart', {
    get() {
      const start = this.get('analysisStart');

      return start ? moment(+start).format(serverDateFormat) : moment().format(serverDateFormat);
    },
    set(key, value) {
      if (!value || value === 'Invalid date') {
        return this.get('analysisStart') || 0;
      }

      const start = moment(value).valueOf();
      this.set('analysisStart', start);

      return moment(value).format(serverDateFormat);
    }
  }),

  // converts analysisEnd from unix ms to serverDateFormat
  anomalyRegionEnd: Ember.computed('analysisEnd', {
    get() {
      const end = this.get('analysisEnd');

      return end ? moment(+end).format(serverDateFormat) : moment().format(serverDateFormat);
    },
    set(key, value) {
      if (!value || value === 'Invalid date') { return this.get('analysisEnd') || 0; }

      const end = moment(value).valueOf();
      this.set('analysisEnd', end);

      return moment(value).format(serverDateFormat);
    }
  }),

  // converts startDate from unix ms to serverDateFormat
  viewRegionStart: Ember.computed(
    'startDate',
    'analysisStart',
    'analysisEnd',
    {
      get() {
        const start = this.get('startDate');

        return start ? moment(+start).format(serverDateFormat) : moment().format(serverDateFormat);
      },
      set(key, value) {
        if (!value || value === 'Invalid date') {
          return this.get('startDate') || 0;
        }

        const start = moment(value).valueOf();
        const analysisStart = this.get('analysisStart');

        if (+start > analysisStart) {
          this.set('analysisStart', undefined);
          this.set('analysisEnd', undefined);
        }

        this.set('startDate', start);
        return moment(value).format(serverDateFormat);
      }
    }
  ),

  // converts endDate from unix ms to serverDateFormat
  viewRegionEnd: Ember.computed(
    'endDate',
    'analysisStart',
    'analysisEnd',
    'model.maxTime',
    {
      get() {
        const end = this.get('endDate');

        return end ? moment(+end).format(serverDateFormat) : moment().format(serverDateFormat);
      },
      set(key, value) {
        if (!value || value === 'Invalid date') { return this.get('endDate') || 0; }

        const maxTime = this.get('model.maxTime');
        const end = moment(value).valueOf();
        const newEnd = (+maxTime < +end) ? maxTime : end;
        const analysisEnd = this.get('analysisEnd');

        if (+newEnd < analysisEnd) {
          this.set('analysisStart', undefined);
          this.set('analysisEnd', undefined);
        }
        this.set('endDate', newEnd);

        return moment(value).format(serverDateFormat);
      }
    }
  ),

  // min date for the anomaly region
  minDate: Ember.computed('startDate', function() {
    const start = this.get('startDate');

    return moment(+start).format(serverDateFormat);
  }),

  // max date for the anomaly region
  maxDate: Ember.computed('endDate', function() {
    const end = this.get('endDate');

    return moment(+end).format(serverDateFormat);
  }),

  uiGranularity: Ember.computed(
    'granularity',
    'model.maxTime',
    'startDate', {
      get() {
        return this.get('granularity');
      },
      // updates dates on granularity change
      set(key, value){
        let endDate = moment(+this.get('model.maxTime'));
        let startDate = 0;
        let analysisEnd = 0;
        let analysisStart = 0;
        let subchartStart = 0;
        let subchartEnd = endDate.clone();


        // Handles this logic here instead of inside SetupController
        // so that query params are updating properly
        if (value === 'DAYS') {
          endDate = endDate.clone().startOf('day');
          analysisEnd = endDate.clone();
          startDate = endDate.clone().subtract(29, 'days').startOf('day').valueOf();
          analysisStart = analysisEnd.clone().subtract('1', 'day');
          subchartStart = endDate.clone().subtract(1, 'week').startOf('day');
        } else if (value === 'HOURS') {
          analysisEnd = endDate.clone().startOf('hour');
          startDate = endDate.clone().subtract(1, 'week').startOf('day').valueOf();
          analysisStart = analysisEnd.clone().subtract('1', 'hour');
          subchartStart =  analysisEnd.clone().subtract('1', 'day').startOf('day');
        } else {
          analysisEnd =  endDate.clone().startOf('hour');
          startDate = endDate.clone().subtract(24, 'hours').startOf('hour').valueOf();
          analysisStart = analysisEnd.clone().subtract('1', 'hour');
          subchartStart = analysisEnd.clone().subtract('3', 'hours').startOf('hour');
        }

        this.setProperties({
          granularity: value,
          startDate,
          endDate: endDate.valueOf(),
          subchartStart: subchartStart.valueOf(),
          subchartEnd: subchartEnd.valueOf(),
          analysisEnd: analysisEnd.valueOf(),
          analysisStart: analysisStart.valueOf()
        });

        return value;
      }
    }),

  actions: {
    // handles graph region date change
    onRegionBrush(start, end) {
      this.setProperties({
        analysisStart: start,
        analysisEnd: end
      });
    },

    // Handles granularity change
    onGranularityChange(granularity) {
      this.set('uiGranularity', granularity);
    },

    // Set new  startDate if applicable
    setNewDate({ start, end }) {
      const rangeStart = moment(start).valueOf();
      const rangeEnd = moment(end).valueOf();

      const {
          startDate: currentStart,
          endDate: currentEnd
        } = this.getProperties('startDate', 'endDate');
      if (rangeStart <= currentStart) {
        const newStartDate = +currentStart - (currentEnd - currentStart);

        this.setProperties({
          startDate: newStartDate
        });
      }
    },

    /**
     * Handles subchart date change (debounced)
     */
    setDateParams([start, end]) {
      Ember.run.debounce(this, this.get('actions.setNewDate'), { start, end }, 2000);
    },

    /**
     * Changes the compare mode
     * @param {String} compareMode baseline compare mode
     */
    onModeChange(compareMode){
      this.set('compareMode', compareMode);
    }
  }
});

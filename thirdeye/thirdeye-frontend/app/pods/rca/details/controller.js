import Ember from 'ember';
import moment from 'moment';

const serverDateFormat = 'YYYY-MM-DD HH:mm';

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

  regionDates: Ember.computed(function(){
    return [this.get('analysisStart'), this.get('analysisEnd')];
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


  uiGranularity: Ember.computed('granularity', {
    get() {
      return this.get('granularity');
    },
    set(key, value){
      this.setProperties({
        granularity: value,
        startDate: undefined,
        endDate: undefined,
        analysisEnd: undefined,
        analysisStart: undefined
      });

      return value;
    }
  }),

  actions: {
    // handles datepicker date changes
    onRegionChange(start, end) {
      this.setProperties({
        anomalyRegionStart: start,
        anomalyRegionEnd: end
      });
    },

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

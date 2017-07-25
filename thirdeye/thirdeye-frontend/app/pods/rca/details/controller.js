import Ember from 'ember';
import moment from 'moment';

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

    // Handles granularity change
    onGranularityChange(granularity) {
      this.set('uiGranularity', granularity);
    },

    // Set new analysis Date and startDate if applicable
    setNewDate({ start, end }) {
      const analysisStart = moment(start).valueOf();
      const analysisEnd = moment(end).valueOf();
      const {
          startDate: currentStart,
          endDate: currentEnd
        } = this.getProperties('startDate', 'endDate');

      if (analysisStart < currentStart) {
        const newStartDate = +currentStart - (currentEnd - currentStart);

        this.setProperties({
          startDate: newStartDate,
          analysisStart,
          analysisEnd
        });
      } else {
        this.setProperties({
          analysisStart,
          analysisEnd
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

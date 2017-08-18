import Ember from 'ember';
import moment from 'moment';

export default Ember.Controller.extend({
  eventStart: 0,
  eventEnd: 0,
  displayStart: 0,
  displayEnd: 0,

  dateFormat: 'MMM D, YYYY hh:mm a',

  actions: {
    // Sets new dimension start and end
    setNewDate({ start, end }) {
      const eventsStart = moment(start).valueOf();
      const eventsEnd = moment(end).valueOf();

      this.setProperties({
        eventsStart,
        eventsEnd
      });
    },

    /**
     * Handles subchart date change (debounced)
     */
    setDateParams([start, end]) {
      Ember.run.debounce(this, this.get('actions.setNewDate'), { start, end }, 500);
    }
  }
});

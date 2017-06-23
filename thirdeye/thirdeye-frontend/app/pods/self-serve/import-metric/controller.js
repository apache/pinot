import Ember from 'ember';

export default Ember.Controller.extend({
  /**
   * Alerts Search Mode
   */
  searchModes: ['function-name', 'alert-group'],

  actions: {
    // Handles alert selection from type ahead
    submit(data) {
      console.log(data);
    }
  }
});

import Ember from 'ember';

export default Ember.Controller.extend({
  /**
   * Alerts Search Mode
   */
  searchModes: ['function-name', 'alert-group'],

  /**
   * Handler for serach by alert group name
   * Utilizing ember concurrency (task)
   */

  allAlertsConfig: Ember.computed.reads('model.allAlertsConfig'),

  // fetchAllAlertConfigs: function* () {
  //   yield timeout(600);
  //   const url = `thirdeye/entity/ALERT_CONFIG`;
  //   return fetch(url)
  //     .then(res => res.json())
  // },

  actions: {
    // Handles alert selection from type ahead
    submit(data) {
      console.log(data);
    }
  }
});

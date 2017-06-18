import Ember from 'ember';
import { task, timeout } from 'ember-concurrency';
import fetch from 'fetch';

export default Ember.Controller.extend({
  /**
   * Alerts Search Mode
   */
  searchModes: ['function-name', 'alert-group'],

  /**
   * Default Search Mode
   */
  selectedSearchMode: 'function-name',

  /**
   * Array of Alerts we're displaying
   */
  selectedAlerts: [],

  /**
   * Handler for serach by function name
   * Utilizing ember concurrency (task)
   */
  searchByFunctionName: task(function* (alert) {
    yield timeout(600);
    const url = `/data/autocomplete/functionByName?name=${alert}`;
    return fetch(url)
      .then(res => res.json())
  }),

  /**
   * Handler for serach by alert group name
   * Utilizing ember concurrency (task)
   */
  searchByAlertGroup: task(function* (alert) {
    yield timeout(600);
    const url = `/data/autocomplete/functionByAlertName?alertName=${alert}`;
    return fetch(url)
      .then(res => res.json())
  }),

  actions: {
    // Handles alert selection from type ahead
    onAlertChange(alerts) {
      this.get('selectedAlerts').pushObject(alerts);
    },

    // Handles UI mode change
    onModeChange(mode) {
      this.set('selectedSearchMode', mode);
    },
  }
});

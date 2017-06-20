import Ember from 'ember';
import { task, timeout } from 'ember-concurrency';
import fetch from 'fetch';

export default Ember.Controller.extend({
  /**
   * Alerts Search Mode
   */
  searchModes: ['Function Name', 'Alert Name'],

  /**
   * True when results appear
   */
  resultsActive: false,

  /**
   * Default Search Mode
   */
  selectedSearchMode: 'Function Name',

  /**
   * Array of Alerts we're displaying
   */
  selectedAlerts: [],

  /**
   * Override existing init function.
   * @method init
   * @param {Object} args - Attributes for this component
   * @return {undefined}
   */
  init(...args) {
    this._super(...args);
    //const alertGroupData = this.fetchAlertGroupData();
    console.log('this for manage controller : ', this.model);

  },

  /**
   * Fetch all Alert Groups
   */
/*  fetchAlertGroupData: task(function* () {
    yield timeout(600);
    const url = `/thirdeye/entity/ALERT_CONFIG`;
    return fetch(url)
      .then(res => res.json())
  }),*/

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
      this.set('resultsActive', true);
    },

    // Handles UI mode change
    onModeChange(mode) {
      this.set('selectedSearchMode', mode);
    },

    removeAll() {
      $('.te-search-results').remove();
      this.set('resultsActive', false);
    }
  }
});

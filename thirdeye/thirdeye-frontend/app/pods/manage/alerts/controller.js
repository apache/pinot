/**
 * Handles alert list and filter settings
 * @module manage/alerts/controller
 * @exports alerts controller
 */
import Ember from 'ember';
import { task, timeout } from 'ember-concurrency';
import fetch from 'fetch';

export default Ember.Controller.extend({
  /**
   * Alerts Search Mode options
   */
  searchModes: ['Alert Name', 'Dataset Name', 'Application Name'],

  /**
   * True when results appear
   */
  resultsActive: false,

  /**
   * Default Search Mode
   */
  selectedSearchMode: 'Alert Name',

  /**
   * Array of Alerts we're displaying
   */
  selectedAlerts: [],

  /**
   * Handler for search by function name
   * Utilizing ember concurrency (task)
   */
  searchByFunctionName: task(function* (alert) {
    yield timeout(600);
    const url = `/data/autocomplete/functionByName?name=${alert}`;
    return fetch(url)
      .then(res => res.json())
  }),

  /**
   * Handler for search by application name
   * Utilizing ember concurrency (task)
   */
  searchByApplicationName: task(function* (alert) {
    yield timeout(600);
    const url = `/data/autocomplete/functionByAppname?appname=${alert}`;
    return fetch(url)
      .then(res => res.json())
  }),

  /**
   * Handler for search by alert dataset name
   * Utilizing ember concurrency (task)
   */
  searchByDatasetName: task(function* (alert) {
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

/**
 * Handles alert list and filter settings
 * @module manage/alerts/controller
 * @exports alerts controller
 */
import Ember from 'ember';
import { task, timeout } from 'ember-concurrency';
import fetch from 'fetch';

export default Ember.Controller.extend({
  queryParams: ['selectedSearchMode'],
  /**
   * Alerts Search Mode options
   */
  searchModes: ['All', 'Alert Name', 'Dataset Name', 'Application Name'],

  /**
   * True when results appear
   */
  resultsActive: false,

  /**
   * Default Search Mode
   */
  selectedSearchMode: 'All',

  /**
   * Array of Alerts we're displaying
   */
  selectedAlerts: [],

  // default current Page
  currentPage: 1,

  // Alerts to display per PAge
  pageSize: 10,

  // Number of pages to display
  paginationSize: 5,

  // Total Number of pages to display
  pagesNum: Ember.computed(
    'selectedAlerts.length',
    'pageSize',
    function() {
      const numAlerts = this.get('selectedAlerts').length;
      const pageSize = this.get('pageSize');
      return Math.ceil(numAlerts/pageSize);
    }
  ),

  // creates the page Array for view
  viewPages: Ember.computed(
    'pages', 
    'currentPage',
    'paginationSize',
    'pageNums',
    function() {
      const size = this.get('paginationSize');
      const currentPage = this.get('currentPage');
      const max = this.get('pagesNum');
      const step = Math.floor(size / 2);

      if (max === 1) { return; }

      const startingNumber = ((max - currentPage) < step) 
        ? max - size + 1
        : Math.max(currentPage - step, 1);
      
      return [...new Array(size)].map((page, index) =>  startingNumber + index);
    }
  ),

  // alerts with pagination
  paginatedSelectedAlerts: Ember.computed(
    'selectedAlerts.@each', 
    'pageSize',
    'currentPage',
    function() {
      const pageSize = this.get('pageSize');
      const pageNumber = this.get('currentPage');
      let alerts = this.get('selectedAlerts');

      return alerts.slice((pageNumber - 1) * pageSize, pageNumber * pageSize);
    }
  ),

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
    onSearchModeChange(mode) {
      if (mode === 'All') { 
        const allAlerts = this.get('model');
        this.setProperties({
          selectedAlerts: allAlerts,
          resultsActive: true
        });
      }
      this.set('selectedSearchMode', mode);
    },

    // removes all
    removeAll() {
      this.set('selectedAlerts', []);
      this.set('resultsActive', false);
    },

    /**
     * action handler for page clicks
     * @param {Number|String} page 
     */
    onPaginationClick(page) {
      let newPage = page;
      let currentPage = this.get('currentPage');

      switch (page) {
        case 'previous':
          newPage = --currentPage;
          break;
        case 'next':
          newPage = ++currentPage;
          break;
      }

      this.set('currentPage', newPage);
    }
  }
});

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
  searchModes: ['All Alerts', 'Alerts', 'Subscriber Groups', 'Applications'],

  /**
   * True when results appear
   */
  resultsActive: false,

  /**
   * Default Search Mode
   */
  selectedSearchMode: 'All Alerts',

  /**
   * Array of Alerts we're displaying
   */
  selectedAlerts: [],
  selectedAll: [],
  selectedSuscriberGroupName: [],
  selectedApplicationName: [],

  // default current Page
  currentPage: 1,

  // Alerts to display per PAge
  pageSize: 10,

  // Number of pages to display
  paginationSize: Ember.computed(
    'pagesNum',
    'pageSize',
    function() {
      const pagesNum = this.get('pagesNum');
      const pageSize = this.get('pageSize');

      return Math.min(pagesNum, pageSize/2);
    }
  ),

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

  suscriberGroupNames: Ember.computed('model.suscriberGroupNames', function() {
    const groupNames = this.get('model.suscriberGroupNames');

    return groupNames
      .filterBy('name')
      .map(group => group.name)
      .uniq()
      .sort();
  }),

  applicationNames: Ember.computed('model.applicationNames', function() {
    const appNames = this.get('model.applicationNames');

    return appNames
      .map(app => app.application)
      .sort();
  }),

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
        ? Math.max(max - size + 1, 1)
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
      .then(res => res.json());
  }),

  /**
   * Handler for search by application name
   * Utilizing ember concurrency (task)
   */
  searchByApplicationName: task(function* (appName) {
    this.set('isLoading', true);
    yield timeout(600);
    const url = `/data/autocomplete/functionByAppname?appname=${appName}`;

    this.set('selectedApplicationName', appName);
    this.set('currentPage', 1);

    return fetch(url)
      .then(res => res.json())
      .then((alerts) => {
        this.set('isLoading', false);
        this.set('selectedAlerts', alerts);
      });
  }),

  /**
   * Handler for search by subscriber gropu name
   * Utilizing ember concurrency (task)
   */
  searchByDatasetName: task(function* (groupName) {
    this.set('isLoading', true);
    yield timeout(600);

    this.set('selectedsuscriberGroupNames', groupName);
    this.set('currentPage', 1);

    const url = `/data/autocomplete/functionByAlertName?alertName=${groupName}`;
    return fetch(url)
      .then(res => res.json())
      .then((filters) => {
        this.set('isLoading', false);
        this.set('selectedAlerts', filters);
      });
  }),

  actions: {
    // Handles alert selection from type ahead
    onAlertChange(alert) {
      if (!alert) { return; }
      this.set('selectedAlerts', [alert]);
      this.set('primaryMetric', alert);
      this.set('resultsActive', true);
    },

    // Handles UI mode change
    onSearchModeChange(mode) {
      if (mode === 'All Alerts') {
        const allAlerts = this.get('model.filters');
        this.setProperties({
          selectedAlerts: allAlerts,
          resultsActive: true
        });
      }
      this.set('selectedSearchMode', mode);
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

/**
 * Handles alert list and filter settings
 * @module manage/alerts/controller
 * @exports alerts controller
 */
import _ from 'lodash';
import fetch from 'fetch';
import {
  set,
  get,
  computed,
  getProperties,
  setProperties
} from '@ember/object';
import moment from 'moment';
import { isPresent } from '@ember/utils';
import Controller from '@ember/controller';
import { reads } from '@ember/object/computed';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import { powerSort } from 'thirdeye-frontend/utils/manage-alert-utils';
import {
  autocompleteAPI,
  yamlAPI
} from 'thirdeye-frontend/utils/api/self-serve';
import { task, timeout } from 'ember-concurrency';
import {
  enrichAlertResponseObject,
  filterToParamsMap,
  populateFiltersLocal
} from 'thirdeye-frontend/utils/yaml-tools';

export default Controller.extend({
  queryParams: ['testMode'],
  detectionHealthQueryTimeRange: [moment().add(1, 'day').subtract(30, 'day').startOf('day').valueOf(), moment().add(1, 'day').startOf('day').valueOf()],

  /**
   * One-way CP to store all sub groups
   */
  totalNumberOfAlerts: 0,
  user: reads('model.user'),
  rules: reads('model.rules'),
  userMail: reads('model.userMail'),

  /**
   * Used to help display filter settings in page header
   */
  primaryFilterVal: 'All Alerts',
  isFilterStrLenMax: false,
  maxFilterStrngLength: 84,

  //
  // internal
  //
  mostRecentSearches: null, // promise

  /**
   * Used to trigger re-render of alerts list
   */
  filtersTriggered: false,

  /**
   * Used to surface newer features pre-launch
   */
  testMode: null,

  /**
   * Boolean to display or hide summary of all filters
   */
  allowFilterSummary: true,

  /**
   * Default Sort Mode
   */
  selectedSortMode: 'Edited:last',
  selectedGlobalFilter: ['All alerts'],

  /**
   * Filter settings
   */
  alertFilters: {},
  resetFiltersGlobal: null,
  resetFiltersLocal: null,

  /**
   * The first and broadest entity search property
   */
  topSearchKeyName: 'application',

  // default current Page
  currentPage: 1,

  // Alerts to display per PAge
  pageSize: 10,

  originalAlerts: [],

  ownedAlerts: computed(
    'originalAlerts',
    function() {
      const {
        originalAlerts,
        user
      } = this.getProperties('originalAlerts', 'userMail');

      return originalAlerts.filter(alert => alert.createdBy === user);
    }
  ),

  filterBlocksGlobal: computed(
    'ownedAlerts',
    'originalAlerts',
    'selectedGlobalFilter',
    function() {
      const {
        selectedGlobalFilter
      } = this.getProperties('selectedGlobalFilter');

      // This filter category is "global" in nature. When selected, they reset the rest of the filters
      return [
        {
          name: 'primary',
          type: 'link',
          preventCollapse: true,
          selected: selectedGlobalFilter,
          filterKeys: ['Alerts I subscribe to', 'Alerts I own', 'All alerts']
        }
      ];
    }
  ),

  initialFiltersLocal: computed(
    'rules',
    'originalAlerts',
    function() {
      const {
        originalAlerts,
        rules
      } = this.getProperties('originalAlerts', 'rules');

      // This filter category is "secondary". To add more, add an entry here and edit the controller's "filterToPropertyMap"
      const filterBlocksLocal = populateFiltersLocal(originalAlerts, rules);
      return filterBlocksLocal;
    }
  ),

  // Number of pages to display
  paginationSize: computed(
    'pagesNum',
    'pageSize',
    function() {
      const pagesNum = this.get('pagesNum');
      const pageSize = this.get('pageSize');

      return Math.min(pagesNum, pageSize/2);
    }
  ),

  // Total Number of pages to display
  pagesNum: computed(
    'totalNumberOfAlerts',
    'pageSize',
    function() {
      const { pageSize, totalNumberOfAlerts } = getProperties(this, 'pageSize', 'totalNumberOfAlerts');
      return Math.ceil(totalNumberOfAlerts/pageSize);
    }
  ),

  // creates the page Array for view
  viewPages: computed(
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

  // Total displayed alerts
  totalFilteredAlerts: computed(
    'originalAlerts.@each',
    function() {
      return this.get('originalAlerts').length;
    }
  ),

  // alerts with pagination
  paginatedSelectedAlerts: computed(
    'originalAlerts.@each',
    'filtersTriggered',
    'pageSize',
    'currentPage',
    'selectedSortMode',
    function() {
      const {
        selectedSortMode
      } = getProperties(this, 'selectedSortMode');
      // Initial set of alerts
      let alerts = this.get('originalAlerts');
      // Alpha sort accounting for spaces in function name
      let alphaSortedAlerts = powerSort(alerts, 'functionName');
      // Handle selected sort order
      switch(selectedSortMode) {
        case 'Edited:first': {
          alerts = alerts.sortBy('id');
          break;
        }
        case 'Edited:last': {
          alerts = alerts = alerts.sortBy('id').reverse();
          break;
        }
        case 'A to Z': {
          alerts = alphaSortedAlerts;
          break;
        }
        case 'Z to A': {
          alerts = alphaSortedAlerts.reverse();
          break;
        }
      }

      for (const alert of alerts) {
        this._fetchDetectionHealth(alert);
      }

      // Return one page of sorted alerts
      return alerts;
    }
  ),

  // Total displayed alerts
  currentPageAlerts: computed(
    'paginatedSelectedAlerts.@each',
    function() {
      return this.get('paginatedSelectedAlerts').length;
    }
  ),

  // String containing all selected filters for display
  activeFiltersString: computed(
    'alertFilters',
    'filtersTriggered',
    function() {
      const alertFilters = get(this, 'alertFilters');
      const filterAbbrevMap = {
        application: 'app',
        subscription: 'group'
      };
      let filterStr = 'All Alerts';
      if (isPresent(alertFilters)) {
        filterStr = alertFilters.primary;
        set(this, 'primaryFilterVal', filterStr);
        let filterArr = [get(this, 'primaryFilterVal')];
        Object.keys(alertFilters).forEach((filterKey) => {
          if (filterKey !== 'primary' && filterKey != 'names') {
            const value = alertFilters[filterKey];
            const isStatusAll = filterKey === 'status' && Array.isArray(value) && value.length > 1;
            // Only display valid search filters
            if (filterKey !== 'triggerType' && value !== null && value.length && !isStatusAll) {
              let concatVal = filterKey === 'status' && !value.length ? 'Active' : value.join(', ');
              let abbrevKey = filterAbbrevMap[filterKey] || filterKey;
              filterArr.push(`${abbrevKey}: ${concatVal}`);
            }
          }
        });
        filterStr = filterArr.join(' | ');
      }
      return filterStr;
    }
  ),

  /**
   * flag meaning alerts are loading for current page
   * @type {Boolean}
   */
  isLoading: computed(
    '_getAlerts.isIdle',
    function() {
      return !get(this, '_getAnomalies.isIdle');
    }
  ),

  /**
   * When user chooses to either find an alert by name, or use a global filter,
   * we should re-set all local filters.
   * @method _resetFilters
   * @param {Boolean} isSelectDisabled
   * @returns {undefined}
   * @private
   */
  _resetLocalFilters() {
    let alertFilters = {};
    const newFilterBlocksLocal = _.cloneDeep(get(this, 'initialFiltersLocal'));

    // Do not highlight any of the primary filters
    Object.assign(alertFilters, { primary: 'none' });

    // Reset local (secondary) filters, and set select fields to 'disabled'
    setProperties(this, {
      filterBlocksLocal: newFilterBlocksLocal,
      resetFiltersLocal: moment().valueOf(),
      allowFilterSummary: false,
      alertFilters
    });
  },

  _getAlerts: task (function * (params) {
    const alerts = yield fetch(yamlAPI.getPaginatedAlertsUrl(params)).then(checkStatus);
    const enrichedAlerts = enrichAlertResponseObject((alerts || {}).elements);
    set(this, 'originalAlerts', enrichedAlerts);
    set(this, 'totalNumberOfAlerts', (alerts || {}).count);
    // Reset secondary filters
    setProperties(this, {
      filterBlocksLocal: _.cloneDeep(get(this, 'initialFiltersLocal')),
      resetFiltersLocal: moment().valueOf()
    });
  }).keepLatest(),

  /**
   * Ember concurrency task that triggers alert names autocomplete
   */
  _searchAlertNames: task(function* (text) {
    yield timeout(1000);
    return fetch(autocompleteAPI.alertByName(text))
      .then(checkStatus);
  }),

  _fetchAlerts() {
    const paramsForAlerts = get(this, 'paramsForAlerts');
    return this.get('_getAlerts').perform(paramsForAlerts);
  },

  async _fetchDetectionHealth(alert) {
    const healthQueryTimeRange = get(this, 'detectionHealthQueryTimeRange');
    const healthUrl = `/detection/health/${alert.id}?start=${healthQueryTimeRange[0]}&end=${healthQueryTimeRange[1]}`;
    const health_result = await fetch(healthUrl).then(checkStatus);
    set(alert, 'health', health_result);
  },

  _handlePrimaryFilter(primaryFilter, paramsForAlerts) {
    switch(primaryFilter) {
      case 'Alerts I subscribe to': {
        const user = this.get('user');
        paramsForAlerts['subscribedBy'] = user;
        set(this, 'paramsForAlerts', paramsForAlerts);
        break;
      }
      case 'Alerts I own': {
        paramsForAlerts['createdBy'] = this.get('userMail');
        set(this, 'selectedGlobalFilter', [primaryFilter]);
        break;
      }
      case 'All alerts': {
        set(this, 'selectedGlobalFilter', [primaryFilter]);
        break;
      }
    }
  },

  _updateParamsWithFilters() {
    const paramsForAlerts = {};
    const alertFilters = this.get('alertFilters');
    const filterFields = Object.keys(filterToParamsMap);
    filterFields.forEach(field => {
      if (Array.isArray(alertFilters[field]) && alertFilters[field].length > 0) {
        if (field === 'status') {
          if (alertFilters[field].length < 2) {
            switch (alertFilters[field][0]) {
              case 'Inactive':
                paramsForAlerts[filterToParamsMap[field]] = false;
                break;
              case 'Active':
                paramsForAlerts[filterToParamsMap[field]] = true;
                break;
            }
          } else {
            delete paramsForAlerts[filterToParamsMap[field]];
          }
        } else {
          paramsForAlerts[filterToParamsMap[field]] = alertFilters[field];
        }
      } else {
        delete paramsForAlerts[filterToParamsMap[field]];
      }
    });
    if (alertFilters.primary) {
      this._handlePrimaryFilter(alertFilters.primary, paramsForAlerts);
    }
    set(this, 'paramsForAlerts', paramsForAlerts);
  },

  actions: {
    // Handles alert selection from single alert typeahead
    onSelectAlertByName(alert) {
      if (!alert) { return; }
      set(this, 'alertFilters', { names: [alert.name] });
      this._updateParamsWithFilters();
      this._fetchAlerts();
      this.set('currentPage', 1);
    },


    /**
     * Performs a search task while cancelling the previous one
     * @param {String} text
     */
    onSearch(text) {
      const searchCache = this.get('mostRecentSearches');
      if (searchCache) {
        searchCache.cancel();
      }
      const task = this.get('_searchAlertNames');
      const taskInstance = task.perform(text);
      set(this, 'searchCache', taskInstance);

      return taskInstance;
    },

    // Handles filter selections (receives array of filter options)
    userDidSelectFilter(filterObj) {
      let alertFilters = get(this, 'alertFilters');
      const updatedAlertFilters = filterObj.primary ? {primary: filterObj.primary} : {primary: alertFilters.primary, ...filterObj};
      setProperties(this, {
        filtersTriggered: true,
        allowFilterSummary: true,
        alertFilters: updatedAlertFilters
      });
      this._updateParamsWithFilters();
      this._fetchAlerts();
      this.set('currentPage', 1);
    },

    /**
     * Send a DELETE request to the function delete endpoint.
     * TODO: Include DELETE postProps in common util function
     * @method removeThirdEyeFunction
     * @param {Number} functionId - The id of the alert to remove
     * @return {Promise}
     */
    removeThirdEyeFunction(functionId) {
      const postProps = {
        method: 'delete',
        headers: { 'content-type': 'text/plain' }
      };
      const url = autocompleteAPI.deleteAlert(functionId);
      fetch(url, postProps).then(checkStatus).then(() => {
        this.send('refreshModel');
      });
    },

    // Handles UI sort change
    onSortModeChange(mode) {
      this.set('selectedSortMode', mode);
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
          if (currentPage > 1) {
            newPage = --currentPage;
          } else {
            newPage = currentPage;
          }
          break;
        case 'next':
          if (currentPage < this.get('pagesNum')) {
            newPage = ++currentPage;
          } else {
            newPage = currentPage;
          }
          break;
      }

      const offset = this.get('pageSize') * (newPage - 1);
      let paramsForAlerts = get(this, 'paramsForAlerts');
      Object.assign(paramsForAlerts, { offset });
      set(this, 'paramsForAlerts', paramsForAlerts);
      this._fetchAlerts();
      this.set('currentPage', newPage);
    }
  }
});

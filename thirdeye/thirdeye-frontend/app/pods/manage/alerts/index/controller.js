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
import { once } from '@ember/runloop';
import { isPresent, isBlank } from '@ember/utils';
import Controller from '@ember/controller';
import { reads } from '@ember/object/computed';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import { powerSort } from 'thirdeye-frontend/utils/manage-alert-utils';
import { selfServeApiCommon } from 'thirdeye-frontend/utils/api/self-serve';

export default Controller.extend({
  queryParams: ['testMode'],

  /**
   * One-way CP to store all sub groups
   */
  originalAlerts: reads('model.alerts'),
  ownedAlerts: reads('model.ownedAlerts'),
  subscribedAlerts: reads('model.subscribedAlerts'),
  initialFiltersLocal: reads('model.initialFiltersLocal'),
  initialFiltersGlobal: reads('model.initialFiltersGlobal'),

  /**
   * Used to help display filter settings in page header
   */
  primaryFilterVal: 'All Alerts',
  isFilterStrLenMax: false,
  maxFilterStrngLength: 84,

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

  /**
   * Filter settings
   */
  resetFiltersGlobal: null,
  resetFiltersLocal: null,
  alertFoundByName: null,

  // Total displayed alerts
  totalFilteredAlerts: 0,

  // default current Page
  currentPage: 1,

  // Alerts to display per PAge
  pageSize: 10,

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
    'totalFilteredAlerts',
    'pageSize',
    function() {
      const { pageSize, totalFilteredAlerts } = getProperties(this, 'pageSize', 'totalFilteredAlerts');
      return Math.ceil(totalFilteredAlerts/pageSize);
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

  // Performs all filters when needed before pagination
  selectedAlerts: computed(
    'filtersTriggered',
    'alertFoundByName',
    'alertFilters',
    function() {
      const {
        alertFilters,
        filterBlocksLocal,
        alertFoundByName,
        filterToPropertyMap,
        originalAlerts: initialAlerts
      } = getProperties(this, 'alertFilters', 'filterBlocksLocal', 'alertFoundByName', 'filterToPropertyMap', 'originalAlerts');
      const filterBlocksCopy = _.cloneDeep(filterBlocksLocal);
      const canRecalcFilterOptions = alertFilters && alertFilters.triggerType !== 'checkbox';
      const inactiveFields = alertFilters ? Object.keys(alertFilters).filter(key => isBlank(alertFilters[key])) : [];
      const filtersToRecalculate = filterBlocksCopy.filter(block => block.type === 'select');
      const nonSelectFilters = filterBlocksCopy.filter(block => block.type !== 'select');
      let filteredAlerts = initialAlerts;

      if (alertFoundByName) {
        filteredAlerts = [alertFoundByName];
      } else if (alertFilters) {
        // Do the filtering of alerts using the original model-fetched alerts array
        filteredAlerts = this._filterAlerts(initialAlerts, alertFilters);
        // Recalculate each select filter's options (based on available filters from current selection)
        if (canRecalcFilterOptions) {
          filtersToRecalculate.forEach((blockItem) => {
            Object.assign(blockItem, { selected: alertFilters[blockItem.name] });
            // We are recalculating each field where options have not been selected
            if (inactiveFields.includes(blockItem.name) || !inactiveFields.length) {
              const alertPropsAsKeys = filteredAlerts.map(alert => alert[filterToPropertyMap[blockItem.name]]);
              const filterKeys = [ ...new Set(powerSort(alertPropsAsKeys, null)) ];
              Object.assign(blockItem, { filterKeys });
            }
          });

          // Preserve selected state for filters that initially have a "selected" property
          if (nonSelectFilters.length) {
            nonSelectFilters.forEach((filter) => {
              filter.selected = alertFilters[filter.name] ? alertFilters[filter.name] : filter.selected;
            });
          }

          // Be sure to update the filter options object once per pass
          once(() => {
            set(this, 'filterBlocksLocal', filterBlocksCopy);
          });
        }
      }

      setProperties(this, {
        filtersTriggered: false, // reset filter trigger
        alertFoundByName: false, // reset single found alert var
        totalFilteredAlerts: filteredAlerts.length
      });

      return filteredAlerts;
    }
  ),

  // alerts with pagination
  paginatedSelectedAlerts: computed(
    'selectedAlerts.@each',
    'filtersTriggered',
    'pageSize',
    'currentPage',
    'selectedSortMode',
    function() {
      const {
        pageSize,
        currentPage,
        selectedSortMode
      } = getProperties(this, 'pageSize', 'currentPage', 'selectedSortMode');
      // Initial set of alerts
      let alerts = this.get('selectedAlerts');
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
      // Return one page of sorted alerts
      return alerts.slice((currentPage - 1) * pageSize, currentPage * pageSize);
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
        if (alertFilters.primary) {
          filterStr = alertFilters.primary;
          set(this, 'primaryFilterVal', filterStr);
        } else {
          let filterArr = [get(this, 'primaryFilterVal')];
          Object.keys(alertFilters).forEach((filterKey) => {
            const value = alertFilters[filterKey];
            const isStatusAll = filterKey === 'status' && Array.isArray(value) && value.length > 1;
            // Only display valid search filters
            if (filterKey !== 'triggerType' && value !== null && value.length && !isStatusAll) {
              let concatVal = filterKey === 'status' && !value.length ? 'Active' : value.join(', ');
              let abbrevKey = filterAbbrevMap[filterKey] || filterKey;
              filterArr.push(`${abbrevKey}: ${concatVal}`);
            }
          });
          filterStr = filterArr.join(' | ');
        }
      }
      return filterStr;
    }
  ),

  /**
   * This is the core filtering method which acts upon a set of initial alerts to return a subset
   * @method _filterAlerts
   * @param {Array} initialAlerts - array of all alerts to start with
   * @param {Object} filters - filter key/values to process
   * @example
   * {
   *   application: ['app name a', 'app name b'],
   *   status: ['active'],
   *   owner: ['person1@linkedin.com, person2@linkedin.com'],
   *   type: null
   * }
   * @returns {undefined}
   * @private
   */
  _filterAlerts(initialAlerts, filters) {
    const filterToPropertyMap = get(this, 'filterToPropertyMap');

    // A click on a primary alert filter will reset 'filteredAlerts'
    if (filters.primary) {
      this._processPrimaryFilters(initialAlerts, filters.primary);
    }

    // Pick up cached alert array for the secondary filters
    let filteredAlerts = get(this, 'filteredAlerts');

    // If there is a secondary filter present, filter by it, using the keys we've set up in our filter map
    Object.keys(filterToPropertyMap).forEach((filterKey) => {
      let filterValueArray = filters[filterKey];
      if (filterValueArray && filterValueArray.length) {
        let newAlerts = filteredAlerts.filter(alert => {
          // See 'filterToPropertyMap' in route. For filterKey = 'owner' this would map alerts by alert['createdBy'] = x
          const targetAlertObject = alert[filterToPropertyMap[filterKey]];
          return targetAlertObject && filterValueArray.includes(targetAlertObject);
        });
        filteredAlerts = newAlerts;
      }
    });

    if (filters.status) {
      // !filters.status.length forces an 'Active' default if user tries to de-select both
      // Depending on the desired UX, remove it if you want to allow user to select NO active and NO inactive.
      const concatStatus = filters.status.length ? filters.status.join().toLowerCase() : 'active';
      const requireAll = filters.status.includes('Active') && filters.status.includes('Inactive');
      const alertsByState = {
        active: filteredAlerts.filter(alert => alert.isActive),
        inactive: filteredAlerts.filter(alert => !alert.isActive)
      };
      // We re-build the alerts array to contain only active alerts, inactive alerts, or both.
      filteredAlerts = requireAll ? [ ...alertsByState.active, ...alertsByState.inactive ] : alertsByState[concatStatus];
    }

    return filteredAlerts;
  },

  /**
   * Simply select the appropriate set of alerts called for by primary filter
   * @method _processPrimaryFilters
   * @param {Array} originalAlerts - array of all alerts from model
   * @param {Object} primaryFilter - filter key/value for primary filter selections
   * @returns {undefined}
   * @private
   */
  _processPrimaryFilters (originalAlerts, primaryFilter) {
    const { ownedAlerts, subscribedAlerts } = getProperties(this, 'ownedAlerts', 'subscribedAlerts');

    let newAlerts = [];
    switch(primaryFilter) {
      case 'Alerts I subscribe to': {
        newAlerts = subscribedAlerts;
        break;
      }
      case 'Alerts I own': {
        newAlerts = ownedAlerts;
        break;
      }
      default: {
        newAlerts = originalAlerts;
      }
    }
    set(this, 'filteredAlerts', newAlerts);
  },

  /**
   * When user chooses to either find an alert by name, or use a global filter,
   * we should re-set all local filters.
   * @method _resetFilters
   * @param {Boolean} isSelectDisabled
   * @returns {undefined}
   * @private
   */
  _resetFilters(isSelectDisabled) {
    // Reset local (secondary) filters, and set select fields to 'disabled'
    setProperties(this, {
      filterBlocksLocal: _.cloneDeep(get(this, 'initialFiltersLocal')),
      resetFiltersLocal: moment().valueOf(),
      isSelectDisabled
    });
    // Reset global (primary) filters, and de-activate any selections
    if (isSelectDisabled) {
      const origFiltersGlobal = get(this, 'initialFiltersGlobal');
      origFiltersGlobal.forEach((filter) => {
        set(filter, 'selected', []);
      });
      setProperties(this, {
        filterBlocksGlobal: origFiltersGlobal,
        resetFiltersGlobal: moment().valueOf()
      });
    }
  },

  actions: {
    // Handles alert selection from single alert typeahead
    onSelectAlertByName(alert) {
      if (!alert) { return; }
      set(this, 'alertFoundByName', alert);
      this._resetFilters(true);
    },

    // Handles filter selections (receives array of filter options)
    userDidSelectFilter(filterArr) {
      setProperties(this, {
        filtersTriggered: true,
        alertFilters: filterArr
      });
      // Reset secondary filters component instance if a primary filter was selected
      if (Object.keys(filterArr).includes('primary')) {
        this._resetFilters(false);
      }
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
      const url = selfServeApiCommon.deleteAlert(functionId);
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

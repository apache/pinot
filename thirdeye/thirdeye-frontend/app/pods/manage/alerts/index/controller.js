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
  alertFilters: {},
  resetFiltersGlobal: null,
  resetFiltersLocal: null,
  alertFoundByName: null,

  /**
   * The first and broadest entity search property
   */
  topSearchKeyName: 'application',

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
        topSearchKeyName,
        filterBlocksLocal,
        alertFoundByName,
        filterToPropertyMap,
        originalAlerts: initialAlerts
      } = getProperties(this, 'alertFilters', 'topSearchKeyName', 'filterBlocksLocal', 'alertFoundByName', 'filterToPropertyMap', 'originalAlerts');
      const filterBlocksCopy = _.cloneDeep(filterBlocksLocal);
      const selectFieldKeys = Object.keys(filterToPropertyMap);
      const fieldsByState = (state) => alertFilters ? selectFieldKeys.filter((key) => {
        return (state === 'active') ? isPresent(alertFilters[key]) : isBlank(alertFilters[key]);
      }) : [];
      const inactiveFields = fieldsByState('inactive');
      const activeFields = fieldsByState('active');
      // Recalculate only 'select' filters when we have a change in them
      const canRecalcFilterOptions = alertFilters && alertFilters.triggerType !== 'checkbox';
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
              Object.assign(blockItem, { filterKeys: this._recalculateFilterKeys(filteredAlerts, blockItem) });
            }
            // For better UX: restore top field options if its the only active field. In our case the top field is 'applications'
            if (blockItem.name === topSearchKeyName && activeFields.join('') === topSearchKeyName) {
              Object.assign(blockItem, { filterKeys: this._recalculateFilterKeys(initialAlerts, blockItem) });
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
        alertFoundByName: false // reset single found alert var
      });

      return filteredAlerts;
    }
  ),

  // Total displayed alerts
  totalFilteredAlerts: computed(
    'selectedAlerts.@each',
    function() {
      return this.get('selectedAlerts').length;
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
          if (filterKey !== 'primary') {
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
   * We are recalculating the options of each selection field. The values come from the aggregated
   * properties across all filtered alerts. For example, it returns all possible values for 'application'
   * @method _recalculateFilterKeys
   * @param {Array} alertsCollection - array of alerts we are extracting values from
   * @param {Object} blockItem - the current search filter object
   * @returns {Array} - a deduped array of values to use as select options
   * @private
   */
  _recalculateFilterKeys(alertsCollection, blockItem) {
    const filterToPropertyMap = get(this, 'filterToPropertyMap');
    // Aggregate all existing values for our target properties in the current array collection
    let alertPropsAsKeys = [];
    // Make sure subscription groups are not bundled for filter parameters
    if (blockItem.name === 'subscription') {
      alertsCollection.forEach(alert => {
        let groups = alert[filterToPropertyMap[blockItem.name]];
        if (groups) {
          groups.split(", ").forEach(g => {
            alertPropsAsKeys.push(g);
          });
        }
      });
    } else if (blockItem.name === 'application') {
      // Make sure applications are not bundled for filter parameters
      alertsCollection.forEach(alert => {
        let applications = alert[filterToPropertyMap[blockItem.name]];
        if (applications) {
          applications.split(", ").forEach(a => {
            alertPropsAsKeys.push(a);
          });
        }
      });
    } else {
      alertPropsAsKeys = alertsCollection.map(alert => alert[filterToPropertyMap[blockItem.name]]);
    }
    // Add 'none' select option if allowed
    const canInsertNullOption = alertPropsAsKeys.includes(undefined) && blockItem.hasNullOption;
    if (canInsertNullOption) { alertPropsAsKeys.push('none'); }
    // Return a deduped array containing all of the values for this property in the current set of alerts
    return [ ...new Set(powerSort(alertPropsAsKeys.filter(val => isPresent(val)), null)) ];
  },

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
          const targetAlertPropertyValue = alert[filterToPropertyMap[filterKey]];
          let alertMeetsCriteria = false;
          // In the cases for subscription and application, there can be multiple values.  We just need to match on one
          if (filterKey === "subscription") {
            if (targetAlertPropertyValue) {
              filterValueArray.forEach(val => {
                if (targetAlertPropertyValue.includes(val)) {
                  alertMeetsCriteria = true;
                }
              });
            }
          } else if (filterKey === "application") {
            if (targetAlertPropertyValue) {
              filterValueArray.forEach(val => {
                if (targetAlertPropertyValue.includes(val)) {
                  alertMeetsCriteria = true;
                }
              });
            }
          } else {
            alertMeetsCriteria = targetAlertPropertyValue && filterValueArray.includes(targetAlertPropertyValue);
          }
          const isMatchForNone = !alert.hasOwnProperty(filterToPropertyMap[filterKey]) && filterValueArray.includes('none');
          return alertMeetsCriteria || isMatchForNone;
        });
        filteredAlerts = newAlerts;
      }
    });

    // If status filter is present, we re-build the results array to contain only active alerts, inactive alerts, or both.
    if (filters.status) {
      const concatStatus = filters.status.length ? filters.status.join().toLowerCase() : 'active';
      const requireAll = filters.status.includes('Active') && filters.status.includes('Inactive');
      const alertsByState = {
        active: filteredAlerts.filter(alert => alert.active),
        inactive: filteredAlerts.filter(alert => !alert.active)
      };
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
  _resetLocalFilters(alert) {
    let alertFilters = {};
    const filterToPropertyMap = get(this, 'filterToPropertyMap');
    const newFilterBlocksLocal = _.cloneDeep(get(this, 'initialFiltersLocal'));

    // Set new select field options (filterKeys) to our found alert properties
    Object.keys(filterToPropertyMap).forEach((filterKey) => {
      let targetAlertProp = alert[filterToPropertyMap[filterKey]];
      alertFilters[filterKey] = targetAlertProp ? [ targetAlertProp ] : ['none'];
      newFilterBlocksLocal.find(filter => filter.name === filterKey).filterKeys = alertFilters[filterKey];
    });

    // Do not highlight any of the primary filters
    Object.assign(alertFilters, { primary: 'none' });

    // Set correct status on current alert
    const alertStatus = alert.active ? 'Active' : 'Inactive';
    newFilterBlocksLocal.find(filter => filter.name === 'status').selected = [ alertStatus ];

    // Reset local (secondary) filters, and set select fields to 'disabled'
    setProperties(this, {
      filterBlocksLocal: newFilterBlocksLocal,
      resetFiltersLocal: moment().valueOf(),
      allowFilterSummary: false,
      alertFilters
    });
  },

  actions: {
    // Handles alert selection from single alert typeahead
    onSelectAlertByName(alert) {
      if (!alert) { return; }
      set(this, 'alertFoundByName', alert);
      this._resetLocalFilters(alert);
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
      // Reset secondary filters component instance if a primary filter was selected
      if (Object.keys(filterObj).includes('primary')) {
        setProperties(this, {
          filterBlocksLocal: _.cloneDeep(get(this, 'initialFiltersLocal')),
          resetFiltersLocal: moment().valueOf()
        });
      }
      // Reset current page
      set(this, 'currentPage', 1);
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

      this.set('currentPage', newPage);
    }
  }
});

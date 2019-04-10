/**
 * Handles alert list and filter settings
 * @module manage/alerts/controller
 * @exports alerts controller
 */
import _ from 'lodash';
import {
  set,
  get,
  computed,
  getProperties,
  setProperties,
  observer
} from '@ember/object';
import { isPresent } from '@ember/utils';
import Controller from '@ember/controller';
import { reads } from '@ember/object/computed';
import { setUpTimeRangeOptions, powerSort } from 'thirdeye-frontend/utils/manage-alert-utils';
import {  anomalyResponseObjNew } from 'thirdeye-frontend/utils/anomaly';
import moment from 'moment';

const TIME_PICKER_INCREMENT = 5; // tells date picker hours field how granularly to display time
const DEFAULT_ACTIVE_DURATION = '1d'; // default duration for time picker timeRangeOptions - see TIME_RANGE_OPTIONS below
const UI_DATE_FORMAT = 'MMM D, YYYY hh:mm a'; // format for date picker to use (usually varies by route or metric)
const DISPLAY_DATE_FORMAT = 'YYYY-MM-DD HH:mm'; // format used consistently across app to display custom date range
const TIME_RANGE_OPTIONS = ['1d', '1w', '1m', '3m'];

export default Controller.extend({

  queryParams: ['testMode'],

  /**
   * One-way CP to store all sub groups
   */
  initialFiltersLocal: reads('model.initialFiltersLocal'),

  /**
   * Used to help display filter settings in page header
   */
  primaryFilterVal: 'All Anomalies',
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
  anomalyFilters: [],
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

  // When the user changes the time range, this will fetch the anomaly ids
  updateVisuals: observer(
    'anomaliesRange',
    function() {
      const {
        anomaliesRange,
        updateAnomalies
      } = this.getProperties('anomaliesRange', 'updateAnomalies');
      set(this, 'isLoading', true);
      const [ start, end ] = anomaliesRange;
      updateAnomalies(start, end)
        .then(res => {
          this.setProperties({
            anomaliesById: res,
            anomalyIds: res.anomalyIds
          });
          this._resetLocalFilters();
          set(this, 'isLoading', false);
        })
        .catch(() => {
          this._resetLocalFilters();
          set(this, 'isLoading', false);
        });

    }
  ),

  // Total Number of pages to display
  pagesNum: computed(
    'totalAnomalies',
    'pageSize',
    function() {
      const { pageSize, totalAnomalies } = getProperties(this, 'pageSize', 'totalAnomalies');
      return Math.ceil(totalAnomalies/pageSize);
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

  // return list of anomalyIds according to filter(s) applied
  selectedAnomalies: computed(
    'anomalyFilters',
    'anomalyIds',
    'activeFiltersString',
    function() {
      const {
        anomalyIds,
        anomalyFilters,
        anomaliesById,
        activeFiltersString
      } = this.getProperties('anomalyIds', 'anomalyFilters', 'anomaliesById', 'activeFiltersString');
      const filterMaps = ['statusFilterMap', 'functionFilterMap', 'datasetFilterMap', 'metricFilterMap', 'dimensionFilterMap'];
      if (activeFiltersString === 'All Anomalies') {
        // no filter applied, just return all
        return anomalyIds;
      }
      let selectedAnomalies = [];
      filterMaps.forEach(map => {
        if (anomalyFilters[map]) {
          // a filter is selected, grab relevant anomalyIds
          if (selectedAnomalies.length === 0) {
            selectedAnomalies = this._unionOfArrays(anomaliesById, map, anomalyFilters[map]);
          } else {
            selectedAnomalies = this._intersectOfArrays(selectedAnomalies, this._unionOfArrays(anomaliesById, map, anomalyFilters[map]));
          }
        }
      });
      return selectedAnomalies;
    }
  ),

  totalAnomalies: computed(
    'selectedAnomalies',
    function() {
      return get(this, 'selectedAnomalies').length;
    }
  ),

  noAnomalies: computed(
    'totalAnomalies',
    function() {
      return (get(this, 'totalAnomalies') === 0);
    }
  ),

  paginatedSelectedAnomalies: computed(
    'selectedAnomalies.@each',
    'filtersTriggered',
    'pageSize',
    'currentPage',
    function() {
      const {
        pageSize,
        currentPage
      } = getProperties(this, 'pageSize', 'currentPage');
      // Initial set of anomalies
      let anomalies = this.get('selectedAnomalies');
      // Return one page of sorted anomalies
      return anomalies.slice((currentPage - 1) * pageSize, currentPage * pageSize);
    }
  ),

  /**
   * Date types to display in the pills
   * @type {Object[]} - array of objects, each of which represents each date pill
   */
  pill: computed(
    'anomaliesRange', 'startDate', 'endDate', 'duration',
    function() {
      const anomaliesRange = get(this, 'anomaliesRange');
      const startDate = Number(anomaliesRange[0]);
      const endDate = Number(anomaliesRange[1]);
      const duration = get(this, 'duration') || DEFAULT_ACTIVE_DURATION;
      const predefinedRanges = {
        'Today': [moment().startOf('day'), moment().startOf('day').add(1, 'days')],
        'Last 24 hours': [moment().subtract(1, 'day'), moment()],
        'Yesterday': [moment().subtract(1, 'day').startOf('day'), moment().startOf('day')],
        'Last Week': [moment().subtract(1, 'week').startOf('day'), moment().startOf('day')]
      };

      return {
        uiDateFormat: UI_DATE_FORMAT,
        activeRangeStart: moment(startDate).format(DISPLAY_DATE_FORMAT),
        activeRangeEnd: moment(endDate).format(DISPLAY_DATE_FORMAT),
        timeRangeOptions: setUpTimeRangeOptions(TIME_RANGE_OPTIONS, duration),
        timePickerIncrement: TIME_PICKER_INCREMENT,
        predefinedRanges
      };
    }
  ),

  // String containing all selected filters for display
  activeFiltersString: computed(
    'anomalyFilters',
    'filtersTriggered',
    function() {
      const anomalyFilters = get(this, 'anomalyFilters');
      const filterAbbrevMap = {
        functionFilterMap: 'function',
        datasetFilterMap: 'dataset',
        statusFilterMap: 'status',
        metricFilterMap: 'metric',
        dimensionFilterMap: 'dimension'
      };
      let filterStr = 'All Anomalies';
      if (isPresent(anomalyFilters)) {
        if (anomalyFilters.primary) {
          filterStr = anomalyFilters.primary;
          set(this, 'primaryFilterVal', filterStr);
        } else {
          let filterArr = [get(this, 'primaryFilterVal')];
          Object.keys(anomalyFilters).forEach((filterKey) => {
            const value = anomalyFilters[filterKey];
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
   * When user chooses to either find an alert by name, or use a global filter,
   * we should re-set all local filters.
   * @method _resetFilters
   * @param {Boolean} isSelectDisabled
   * @returns {undefined}
   * @private
   */
  _resetLocalFilters() {
    let anomalyFilters = {};
    const newFilterBlocksLocal = _.cloneDeep(get(this, 'initialFiltersLocal'));
    const anomaliesById = get(this, 'anomaliesById');

    // Fill in select options for these filters ('filterKeys') based on alert properties from model.alerts
    newFilterBlocksLocal.forEach((filter) => {
      if (filter.name === "dimensionFilterMap") {
        const anomalyPropertyArray = Object.keys(anomaliesById.searchFilters[filter.name]);
        let filterKeys = [];
        anomalyPropertyArray.forEach(dimensionType => {
          let group = Object.keys(anomaliesById.searchFilters[filter.name][dimensionType]);
          group = group.map(dim => `${dimensionType}::${dim}`);
          filterKeys = [...filterKeys, ...group];
        });
        Object.assign(filter, { filterKeys });
      } else if (filter.name === "statusFilterMap"){
        let anomalyPropertyArray = Object.keys(anomaliesById.searchFilters[filter.name]);
        anomalyPropertyArray = anomalyPropertyArray.map(prop => {
          // get the right object
          const mapping = anomalyResponseObjNew.filter(e => (e.status === prop));
          // map the status to name
          return mapping.length > 0 ? mapping[0].name : prop;
        });
        const filterKeys = [ ...new Set(powerSort(anomalyPropertyArray, null))];
        // Add filterKeys prop to each facet or filter block
        Object.assign(filter, { filterKeys });
      } else {
        const anomalyPropertyArray = Object.keys(anomaliesById.searchFilters[filter.name]);
        const filterKeys = [ ...new Set(powerSort(anomalyPropertyArray, null))];
        // Add filterKeys prop to each facet or filter block
        Object.assign(filter, { filterKeys });
      }
    });

    // Reset local (secondary) filters, and set select fields to 'disabled'
    setProperties(this, {
      filterBlocksLocal: newFilterBlocksLocal,
      resetFiltersLocal: moment().valueOf(),
      anomalyFilters
    });
  },

  // method to union anomalyId arrays for filters applied of same type
  _unionOfArrays(anomaliesById, filterType, selectedFilters) {
    //handle dimensions separately, since they are nested
    if (filterType === 'dimensionFilterMap') {
      let addedIds = [];
      selectedFilters.forEach(filter => {
        const [type, dimension] = filter.split('::');
        addedIds = [...addedIds, ...anomaliesById.searchFilters.dimensionFilterMap[type][dimension]];
      });
      return addedIds;
    } else if (filterType === 'statusFilterMap'){
      let addedIds = [];
      const translatedFilters = selectedFilters.map(f => {
        // get the right object
        const mapping = anomalyResponseObjNew.filter(e => (e.name === f));
        // map the name to status
        return mapping.length > 0 ? mapping[0].status : f;
      });
      translatedFilters.forEach(filter => {
        addedIds = [...addedIds, ...anomaliesById.searchFilters[filterType][filter]];
      });
      return addedIds;
    } else {
      let addedIds = [];
      selectedFilters.forEach(filter => {
        addedIds = [...addedIds, ...anomaliesById.searchFilters[filterType][filter]];
      });
      return addedIds;
    }
  },

  // method to intersect anomalyId arrays for filters applied of different types
  // i.e. we want anomalies that have both characteristics when filter type is different
  _intersectOfArrays(existingArray, incomingArray) {
    return existingArray.filter(anomalyId => incomingArray.includes(anomalyId));
  },

  actions: {
    // Handles filter selections (receives array of filter options)
    userDidSelectFilter(filterObj) {
      const filterBlocksLocal = get(this, 'filterBlocksLocal');
      filterBlocksLocal.forEach(block => {
        block.selected = filterObj[block.name];
      });
      setProperties(this, {
        filtersTriggered: true,
        allowFilterSummary: true,
        anomalyFilters: filterObj
      });
      // Reset current page
      set(this, 'currentPage', 1);
    },

    /**
     * Sets the new custom date range for anomaly coverage
     * @method onRangeSelection
     * @param {Object} rangeOption - the user-selected time range to load
     */
    onRangeSelection(timeRangeOptions) {
      const {
        start,
        end,
        value: duration
      } = timeRangeOptions;

      const startDate = moment(start).valueOf();
      const endDate = moment(end).valueOf();
      //Update the time range option selected
      set(this, 'anomaliesRange', [startDate, endDate]);
      set(this, 'duration', duration);
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

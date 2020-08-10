/**
 * Handles alert list and filter settings
 * @module manage/alerts/controller
 * @exports alerts controller
 */
import {computed, get, getProperties, set, setProperties} from '@ember/object';
import {inject as service} from '@ember/service';
import {isPresent} from '@ember/utils';
import Controller from '@ember/controller';
import {reads} from '@ember/object/computed';
import {setUpTimeRangeOptions} from 'thirdeye-frontend/utils/manage-alert-utils';
import {searchAnomalyWithFilters} from 'thirdeye-frontend/utils/anomaly';
import moment from 'moment';

const TIME_PICKER_INCREMENT = 5; // tells date picker hours field how granularly to display time
const DEFAULT_ACTIVE_DURATION = '1d'; // default duration for time picker timeRangeOptions - see TIME_RANGE_OPTIONS below
const UI_DATE_FORMAT = 'MMM D, YYYY hh:mm a'; // format for date picker to use (usually varies by route or metric)
const DISPLAY_DATE_FORMAT = 'YYYY-MM-DD HH:mm'; // format used consistently across app to display custom date range
const TIME_RANGE_OPTIONS = ['1d', '1w', '1m', '3m'];

export default Controller.extend({

  queryParams: ['testMode'], store: service('store'),

  notifications: service('toast'),

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
  anomalyFilters: {}, resetFiltersLocal: null, alertFoundByName: null,

  /**
   * The first and broadest entity search property
   */
  topSearchKeyName: 'application',

  // default current Page
  currentPage: 1,

  // Alerts to display per PAge
  pageSize: 10,

  // Number of pages to display
  paginationSize: computed('pagesNum', 'pageSize', function () {
    const pagesNum = this.get('pagesNum');
    const pageSize = this.get('pageSize');

    return Math.min(pagesNum, pageSize / 2);
  }),

  // Total Number of pages to display
  pagesNum: computed('totalAnomalies', 'pageSize', function () {
    const {pageSize, totalAnomalies} = getProperties(this, 'pageSize', 'totalAnomalies');
    return Math.ceil(totalAnomalies / pageSize);
  }),

  // creates the page Array for view
  viewPages: computed('pages', 'currentPage', 'paginationSize', 'pageNums', function () {
    const size = this.get('paginationSize');
    const currentPage = this.get('currentPage');
    const max = this.get('pagesNum');
    const step = Math.floor(size / 2);

    if (max === 1) {
      return;
    }

    const startingNumber = ((max - currentPage) < step) ? Math.max(max - size + 1, 1) : Math.max(currentPage - step, 1);

    return [...new Array(size)].map((page, index) => startingNumber + index);
  }),

  totalAnomalies: computed('searchResult', function () {
    return get(this, 'searchResult').count;
  }),

  noAnomalies: computed('totalAnomalies', function () {
    return (get(this, 'totalAnomalies') === 0);
  }),

  paginatedSelectedAnomalies: computed('searchResult', function () {
    // Return one page of sorted anomalies
    return get(this, 'searchResult').elements;
  }),

  /**
   * Date types to display in the pills
   * @type {Object[]} - array of objects, each of which represents each date pill
   */
  pill: computed('anomaliesRange', 'startDate', 'endDate', 'duration', function () {
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
  }),

  // String containing all selected filters for display
  activeFiltersString: computed('anomalyFilters', 'filtersTriggered', function () {
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
    return filterStr;
  }),

  // When the user changes the time range, this will fetch the anomaly ids
  _updateVisuals() {
    const {
      anomaliesRange, anomalyIds, pageSize, currentPage, anomalyFilters
    } = this.getProperties('anomaliesRange', 'anomalyIds', 'pageSize', 'currentPage', 'anomalyFilters');

    set(this, 'isLoading', true);
    const [start, end] = anomaliesRange;
    searchAnomalyWithFilters(pageSize * (currentPage - 1), pageSize, anomalyIds ? null : start, anomalyIds ? null : end,
      anomalyFilters.feedbackStatus, anomalyFilters.subscription, anomalyFilters.alertName, anomalyFilters.metric,
      anomalyFilters.dataset, anomalyIds)
      .then(res => {
        this.setProperties({
          searchResult: res
        });
        set(this, 'isLoading', false);
      })
      .catch(() => {
        set(this, 'isLoading', false);
      });
  },

  actions: {
    // Clears all selected filters at once
    clearFilters() {
      set(this, 'anomalyFilters', {});
      this._updateVisuals();
    },

    // Handles filter selections (receives array of filter options)
    userDidSelectFilter(filterObj) {
      setProperties(this, {
        filtersTriggered: true, allowFilterSummary: true, anomalyFilters: filterObj
      });
      // Reset current page
      set(this, 'currentPage', 1);
      this._updateVisuals();
    },

    /**
     * Sets the new custom date range for anomaly coverage
     * @method onRangeSelection
     * @param {Object} rangeOption - the user-selected time range to load
     */
    onRangeSelection(timeRangeOptions) {
      const {
        start, end, value: duration
      } = timeRangeOptions;

      const startDate = moment(start).valueOf();
      const endDate = moment(end).valueOf();
      //Update the time range option selected
      set(this, 'anomaliesRange', [startDate, endDate]);
      set(this, 'duration', duration);
      set(this, 'anomalyIds', null);
      set(this, 'currentPage', 1)
      this._updateVisuals();
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
      this._updateVisuals();
    }
  }
});

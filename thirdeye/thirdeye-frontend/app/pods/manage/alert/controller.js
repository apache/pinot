/**
 * Controller for Alert Landing and Details Page
 * @module manage/alert
 * @exports manage/alert
 */
import fetch from 'fetch';
import Ember from 'ember';
import moment from 'moment';
import _ from 'lodash';
import { checkStatus } from 'thirdeye-frontend/helpers/utils';

export default Ember.Controller.extend({
  /**
   * Set up to receive prompt to trigger page mode change.
   * When replay is received as true, it indicates that this is a
   * newly created alert and replay is needed in order to display anomaly data.
   */
  queryParams: ['replay', 'replayId', 'duration', 'startDate'],
  replay: false,
  replayId: null,
  duration: null,
  startDate: null,

  /**
   * Standard legend settings for graph
   */
  legendText: {
    dotted: {
      text: 'WoW'
    },
    solid: {
      text: 'Observed'
    }
  },

  /**
   * Mapping table column names to corresponding prop keys
   */
  sortMap: {
    start: 'anomalyStart',
    score: 'severityScore',
    change: 'changeRate',
    resolution: 'anomalyFeedback'
  },

  /**
   * Date format for date range picker
   */
  serverDateFormat: 'YYYY-MM-DD HH:mm',

  /**
   * Set initial view values
   * @method initialize
   * @param {Boolean} isReplayNeeded
   * @return {undefined}
   */
  initialize(isReplayNeeded) {
    this.setProperties({
      filters: {},
      metricData: {},
      graphConfig: {},
      loadedWowData: [],
      predefinedRanges: {},
      selectedSortMode: '',
      selectedTimeRange: '',
      selectedFilters: JSON.stringify({}),
      isAlertReady: false,
      isReplayStarted: true,
      isReplayPending: false,
      isReplayDone: false,
      isMetricDataLoading: true,
      isReplayStatusError: false,
      isReplayModeWrapper: true,
      isAnomalyArrayChanged: false,
      requestCanContinue: true,
      sortColumnStartUp: false,
      sortColumnScoreUp: false,
      sortColumnChangeUp: false,
      sortColumnResolutionUp: false,
      baselineOptions: [{ name: 'Predicted', isActive: true }],
      selectedDimension: 'All Dimensions',
      selectedResolution: 'All Resolutions',
      dateRangeToRender: [30, 10, 5],
      currentPage: 1,
      pageSize: 10
    });
    // Toggle page mode if replay is needed
    if (isReplayNeeded) {
      this.set('isReplayPending', true);
      this.triggerReplay(this.get('alertId'));
    }
  },

  /**
   * Number of pages to display
   */
  paginationSize: Ember.computed(
    'pagesNum',
    'pageSize',
    function() {
      const { pagesNum, pageSize } = this.getProperties('pagesNum', 'pageSize');
      return Math.min(pagesNum, pageSize/2);
    }
  ),

  /**
   * Total Number of pages to display
   */
  pagesNum: Ember.computed(
    'filteredAnomalies',
    'pageSize',
    function() {
      const { filteredAnomalies, pageSize } = this.getProperties('filteredAnomalies', 'pageSize');
      const anomalyCount = filteredAnomalies.length || 0;
      return Math.ceil(anomalyCount/pageSize);
    }
  ),

  /**
   * Creates the page Array for view
   */
  viewPages: Ember.computed(
    'pages',
    'currentPage',
    'paginationSize',
    'pagesNum',
    function() {
      const {
        currentPage,
        pagesNum: max,
        paginationSize: size
      } = this.getProperties('currentPage', 'pagesNum', 'paginationSize');
      const step = Math.floor(size / 2);

      if (max === 1) { return; }

      const startingNumber = ((max - currentPage) < step)
        ? Math.max(max - size + 1, 1)
        : Math.max(currentPage - step, 1);

      return [...new Array(size)].map((page, index) =>  startingNumber + index);
    }
  ),

  /**
   * Pre-filtered and sorted anomalies with pagination
   */
  paginatedFilteredAnomalies: Ember.computed(
    'filteredAnomalies.@each',
    'pageSize',
    'currentPage',
    'loadedWoWData',
    'selectedSortMode',
    function() {
      let anomalies = this.get('filteredAnomalies');
      const { pageSize, currentPage, selectedSortMode } = this.getProperties('pageSize', 'currentPage', 'selectedSortMode');

      if (selectedSortMode) {
        let [ sortKey, sortDir ] = selectedSortMode.split(':');

        if (sortDir === 'up') {
          anomalies = anomalies.sortBy(this.get('sortMap')[sortKey]);
        } else {
          anomalies = anomalies.sortBy(this.get('sortMap')[sortKey]).reverse();
        }
      }

      return anomalies.slice((currentPage - 1) * pageSize, currentPage * pageSize);
    }
  ),

  /**
   * Indicates the allowed date range picker increment based on granularity
   * @type {Number}
   */
  timePickerIncrement: Ember.computed('alertData.windowUnit', function() {
    const granularity = this.get('alertData.windowUnit').toLowerCase();

    switch(granularity) {
      case 'DAYS':
        return 1440;
      case 'HOURS':
        return 60;
      default:
        return 5;
    }
  }),

  /**
   * Indicates the allowed date range picker increment based on granularity
   * @type {Boolean}
   */
  showGraph: Ember.computed.bool('isGraphReady'),

  /**
   * Returns a time object for the date-time-picker from selected range end date
   * @type {Object}
   */
  viewRegionEnd: Ember.computed(
    'activeRangeEnd',
    function() {
      const end = this.get('activeRangeEnd');
      return moment(end).format(this.get('serverDateFormat'));
    }
  ),

  /**
   * Returns a time object for the date-time-picker from selected range start date
   * @type {Object}
   */
  viewRegionStart: Ember.computed(
    'activeRangeStart',
    function() {
      const start = this.get('activeRangeStart');
      return moment(start).format(this.get('serverDateFormat'));
    }
  ),

  /**
   * Indicates the date format to be used based on granularity
   * @type {String}
   */
  uiDateFormat: Ember.computed('alertData.windowUnit', function() {
    const granularity = this.get('alertData.windowUnit').toLowerCase();

    switch(granularity) {
      case 'DAYS':
        return 'MMM D, YYYY';
      case 'HOURS':
        return 'MMM D, YYYY h a';
      default:
        return 'MMM D, YYYY hh:mm a';
    }
  }),

  /**
   * Data needed to render the stats 'cards' above the anomaly graph for this alert
   * @type {Object}
   */
  anomalyStats: Ember.computed(
    'totalAnomalies',
    function() {
      const total = this.get('totalAnomalies') || 0;
      const anomalyStats = [
        {
          title: 'Number of anomalies',
          text: 'Estimated average number of anomalies per month',
          value: total,
          projected: '5'
        },
        {
          title: 'Response rate',
          text: 'Percentage of anomalies that has a response',
          value: '87.1%'
        },
        {
          title: 'Precision',
          text: 'Among all anomalies detected, the percentage of them that are true.',
          value: '50%',
          projected: '89.2%'
        },
        {
          title: 'Recall',
          text: 'Among all anomalies that happened, the percentage of them detected by the system',
          value: '25%',
          projected: '89.2%'
        },
        {
          title: 'MTTD for >30% change',
          text: 'Minimum time to detect for anomalies with > 30% change',
          value: '4.8 mins',
          projected: '5 mins'
        }
      ];
      return anomalyStats;
    }
  ),

  /**
   * If user selects a dimension from the dropdown, we filter the anomaly results here.
   * NOTE: this is currently set up to support single-dimension filters
   * @type {Object}
   */
  filteredAnomalies: Ember.computed(
    'selectedDimension',
    'selectedResolution',
    'anomalyData',
    function() {
      const targetDimension = this.get('selectedDimension');
      const targetResolution = this.get('selectedResolution');
      let anomalies = this.get('anomalyData');

      // Filter for selected dimension
      if (targetDimension !== 'All Dimensions') {
        anomalies = anomalies.filter((data) => {
          return targetDimension === `${data.dimensionList[0].dimensionKey}:${data.dimensionList[0].dimensionVal}`;
        });
      }

      // Filter for selected resolution
      if (targetResolution !== 'All Resolutions') {
        anomalies = anomalies.filter((data) => {
          return targetResolution === data.anomalyFeedback;
        });
      }

      return anomalies;
    }
  ),

  /**
   * Placeholder for options for range field. Here we generate arbitrary date ranges from our config.
   * @type {Array}
   */
  rangeOptionsExample: Ember.computed(
    'dateRangeToRender',
    function() {
      return this.get('dateRangeToRender').map(this.renderDate);
    }
  ),

  /**
   * Generate date range selection options if needed
   * @method renderDate
   * @param {Number} range - number of days (duration)
   * @return {String}
   */
  renderDate: function(range) {
    // TODO: enable single day range
    const newDate = moment().subtract(range, 'days').endOf('day').utc().format("DD MM YYY");
    return `Last ${range} Days (${newDate} to Today)`;
  },

  /**
   * Fetches change rate data for each available anomaly id
   * @method fetchCombinedAnomalyChangeData
   * @returns {Ember.RSVP promise}
   */
  fetchCombinedAnomalyChangeData() {
    let promises = {};

    for (var anomaly of this.get('anomalyData')) {
      let id = anomaly.anomalyId;
      promises[id] = fetch(`/anomalies/${id}`).then(checkStatus);
    }

    return Ember.RSVP.hash(promises);
  },

  /**
   * Pings the job-info endpoint to check status of an ongoing replay job.
   * If there is no progress after a set time, we display an error message.
   * TODO: Set error message on timeout
   * @method checkReplayStatus
   * @param {Number} jobId - the id for the newly triggered replay job
   * @return {fetch promise}
   */
  checkReplayStatus(jobId) {
    const checkStatusUrl = `/thirdeye-admin/job-info/job/${jobId}/status`;
    const timerStart = moment();

    fetch(checkStatusUrl).then(checkStatus).then((status) => {
      if (status.toLowerCase() === 'completed') {
        this.set('isReplayPending', false);
      } else if (this.get('requestCanContinue')) {
        Ember.run.later(this, function() {
          this.checkReplayStatus(jobId);
        }, 5000);
      }
    });
  },

  /**
   * Downloads data that is not critical for the initial page load
   * TODO: Should we move all requests to the route?
   * @method fetchDeferredAnomalyData
   * @return {Promise}
   */
  fetchDeferredAnomalyData() {
    const wowOptions = ['Wow', 'Wo2W', 'Wo3W', 'Wo4W'];
    const { anomalyData, baselineOptions } = this.getProperties('anomalyData', 'baselineOptions');
    const newWowList = wowOptions.map((item) => {
      return { name: item, isActive: false };
    });

    return this.fetchCombinedAnomalyChangeData()
      .then((wowData) => {
        anomalyData.forEach((anomaly) => {
          anomaly.wowData = wowData[anomaly.anomalyId] || {};
        });
        // Display rest of options once data is loaded ('2week', 'Last Week')
        this.set('baselineOptions', [baselineOptions[0], ...newWowList]);
        return fetch(this.get('metricDataUrl')).then(checkStatus);
      })
      .then((metricData) => {
        // Display graph once data has loaded
        this.setProperties({
          isGraphReady: true,
          isMetricDataLoading: false,
          metricData
        });
      })
      .catch((errors) => {
        this.setProperties({
          loadError: true,
          loadErrorMsg: errors
        });
      });
  },

  /**
   * Sends a request to begin advanced replay for a metric. The replay will fetch new
   * time-series data based on user-selected sensitivity settings.
   * @method triggerReplay
   * @param {Number} functionId - the id for the newly created function (alert)
   * @return {Ember.RSVP.Promise}
   */
  triggerReplay(functionId) {
    const emailData = this.get('emailData')[0];
    const subject = `Anomaly results processed for ${functionId}`;
    const startTime = moment().subtract(1, 'month').endOf('day').utc().format("YYYY-MM-DD");
    const endTime = moment().subtract(1, 'day').endOf('day').utc().format("YYYY-MM-DD");
    const granularity = this.get('alertData.windowUnit').toLowerCase();
    const isDailyOrHourly = ['day', 'hour'].includes(granularity);
    const speedUp = !(granularity.includes('hour') || granularity.includes('day'));
    const recipients = emailData ? encodeURIComponent(emailData.recipients.replace(/,{2,}/g, '')) : '';
    const sensitivity = this.get('alertData.alertFilter.sensitivity') || isDailyOrHourly ? 'Sensitive' : 'Medium';
    const pattern = this.get('alertData.alertFilter.pattern') || null;
    const replayWrapperUrl = `/detection-job/${functionId}/notifyreplaytuning?start=${startTime}` +
      `&end=${endTime}&speedup=${speedUp}&userDefinedPattern=${pattern}&sensitivity=${sensitivity}` +
      `&removeAnomaliesInWindow=true&to=${recipients}`;
    const replayStartUrl = `/detection-job/${functionId}/replay?start=${startTime}&end=${endTime}&speedup=${speedUp}`;
    const postProps = {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      }
    };

    // Two ways to trigger replay
    if (this.get('isReplayModeWrapper')) {
      fetch(replayWrapperUrl, postProps).then((res) => checkStatus(res, 'post')).then(() => {
        this.set('isReplayPending', false);
      }).catch(() => {
        this.set('isReplayStatusError', true);
      });
    } else {
      fetch(replayStartUrl, postProps).then((res) => checkStatus(res, 'post')).then((response) => {
        response.json().then((jobId) => {
          this.checkReplayStatus(Object.values(jobId)[0]);
        });
      }).catch(() => {
        this.set('isReplayStatusError', true);
      });
    }
  },

  /**
   * Update feedback status on any anomaly
   * @method updateAnomalyFeedback
   * @param {Number} anomalyId - the id of the anomaly to update
   * @param {String} feedbackType - key for feedback type
   * @return {Ember.RSVP.Promise}
   */
  updateAnomalyFeedback(anomalyId, feedbackType) {
    const url = `/anomalies/updateFeedback/${anomalyId}`;
    const postProps = {
      method: 'post',
      body: JSON.stringify({ feedbackType, comment: '' }),
      headers: { 'content-type': 'Application/Json' }
    };
    return fetch(url, postProps).then((res) => checkStatus(res, 'post'));
  },

  /**
   * Fetch a single anomaly record for verification
   * @method verifyAnomalyFeedback
   * @param {Number} anomalyId
   * @return {undefined}
   */
  verifyAnomalyFeedback(anomalyId) {
    const anomalyUrl = this.get('anomalyDataUrl') + anomalyId;
    return fetch(anomalyUrl).then(checkStatus);
  },

  /**
   * Reset all time range options and activate the selected one
   * @method newTimeRangeOptions
   * @param {String} activeKey - label for currently active time range
   * @return {undefined}
   */
  newTimeRangeOptions(activeKey) {
    const timeRangeOptions = this.get('timeRangeOptions');
    const newOptions = timeRangeOptions.map((range) => {
      return {
        name: range.name,
        value: range.value,
        isActive: false
      };
    });
    const foundRangeOption = newOptions.find((range) => range.value === activeKey);

    if (foundRangeOption) {
      foundRangeOption.isActive = true;
    }

    return newOptions;
  },

  /**
   * When exiting route, lets kill the replay status check calls
   * @method clearAll
   * @return {undefined}
   */
  clearAll() {
    this.setProperties({
      requestCanContinue: false
    });
  },

  /**
   * Actions for alert page
   */
  actions: {

    /**
     * Handle selected dimension filter
     * @method onSelectDimension
     * @param {Object} selectedObj - the user-selected dimension to filter by
     * @return {undefined}
     */
    onSelectDimension(selectedObj) {
      this.set('selectedDimension' , selectedObj);
    },

    /**
     * Handle selected resolution filter
     * @method onSelectResolution
     * @param {Object} selectedObj - the user-selected resolution to filter by
     * @return {undefined}
     */
    onSelectResolution(selectedObj) {
      this.set('selectedResolution', selectedObj);
    },

    /**
     * Handle dynamically saving anomaly feedback responses
     * @method onChangeAnomalyResponse
     * @param {Object} anomalyRecord - the anomaly being responded to
     * @param {String} selectedResponse - user-selected anomaly feedback option
     * @param {Object} inputObj - the selection object
     * @return {undefined}
     */
    onChangeAnomalyResponse(anomalyRecord, selectedResponse, inputObj) {
      const responseObj = _.find(this.get('anomalyResponseObj'), { 'name': selectedResponse });
      Ember.set(inputObj, 'selected', selectedResponse);

      // Save anomaly feedback
      this.updateAnomalyFeedback(anomalyRecord.anomalyId, responseObj.value)
        .then((res) => {
          // We make a call to ensure our new response got saved
          this.verifyAnomalyFeedback(anomalyRecord.anomalyId, responseObj.status)
            .then((res) => {
              const filterMap = res.searchFilters ? res.searchFilters.statusFilterMap : null;
              if (filterMap && filterMap.hasOwnProperty(responseObj.status)) {
                Ember.set(anomalyRecord, 'anomalyFeedback', selectedResponse);
                Ember.set(anomalyRecord, 'showResponseSaved', true);
              } else {
                throw new Error('Response not saved');
              }
            }) // verifyAnomalyFeedback
        }) // updateAnomalyFeedback
        .catch((err) => {
          Ember.set(anomalyRecord, 'showResponseFailed', true);
          Ember.set(anomalyRecord, 'showResponseSaved', false);
        });
    },

    /**
     * Action handler for page clicks
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
    },

    /**
     * Handle display of selected baseline options
     * @method onBaselineOptionClick
     * @param {Object} wowObj - the baseline selection
     * @return {undefined}
     */
    onBaselineOptionClick(wowObj) {
      const { anomalyData, baselineOptions } = this.getProperties('anomalyData', 'baselineOptions');
      const isValidSelection = !wowObj.isActive;
      let newOptions = baselineOptions.map((val) => {
        return { name: val.name, isActive: false };
      });

      // Set active option
      newOptions.find((val) => val.name === wowObj.name).isActive = true;
      this.set('baselineOptions', newOptions);

      // Set new values for each anomaly
      if (isValidSelection) {
        for (var anomaly of anomalyData) {
          const wow = anomaly.wowData;
          const wowDetails = _.find(wow.compareResults, { 'compareMode': wowObj.name });
          let curr = anomaly.current;
          let base = anomaly.baseline;
          let change = anomaly.changeRate;

          if (wowDetails) {
            curr = wow.currentVal.toFixed(2);
            base = wowDetails.baselineValue.toFixed(2);
            change = wowDetails.change.toFixed(2);
          }

          Ember.set(anomaly, 'shownCurrent', curr);
          Ember.set(anomaly, 'shownBaseline', base);
          Ember.set(anomaly, 'shownChangeRate', change);
        }
      }
    },

    /**
     * Handle display of selected anomaly time ranges (reload the model with new query params)
     * @method onRangeOptionClick
     * @param {Object} rangeOption - the selected range object
     * @return {undefined}
     */
    onRangeOptionClick(rangeOption) {
      const rangeFormat = 'YYYY-MM-DD';
      const defaultEndDate = moment().subtract(1, 'day').endOf('day').valueOf();

      // Trigger reload in model with new time range. Transition for 'custom' dates is handled by 'onRangeSelection'
      if (rangeOption.value !== 'custom') {
        this.setProperties({
          timeRangeOptions: this.newTimeRangeOptions(rangeOption.value),
          activeRangeStart: moment(rangeOption.start).format(rangeFormat),
          activeRangeEnd: moment(defaultEndDate).format(rangeFormat)
        });
        this.transitionToRoute({ queryParams: {
          duration: rangeOption.value,
          startDate: rangeOption.start,
          endDate: defaultEndDate
        }});
      }
    },

    /**
     * Sets the new custom date range for anomaly coverage
     * @method onRangeSelection
     * @param {String} start  - stringified start date
     * @param {String} end    - stringified end date
     * @return {undefined}
     */
    onRangeSelection(start, end) {
      this.set('timeRangeOptions', this.newTimeRangeOptions('custom'));
      this.transitionToRoute({ queryParams: {
        duration: 'custom',
        startDate: moment(start).valueOf(),
        endDate: moment(end).valueOf()
      }});
    },

    /**
     * Placeholder for subscribe button click action
     * @method onClickAlertSubscribe
     * @return {undefined}
     */
    onClickAlertSubscribe() {
      // TODO: Set user as watcher for this alert
    },

    /**
     * Handle sorting for each sortable table column
     * @method toggleSortDirection
     * @param {String} sortKey  - stringified start date
     * @return {undefined}
     */
    toggleSortDirection(sortKey) {
      const propName = 'sortColumn' + sortKey.capitalize() + 'Up' || '';

      this.toggleProperty(propName);
      if (this.get(propName)) {
        this.set('selectedSortMode', sortKey + ':up');
      } else {
        this.set('selectedSortMode', sortKey + ':down');
      }

      //On sort, set table to first pagination page
      this.set('currentPage', 1);
    }

  }
});

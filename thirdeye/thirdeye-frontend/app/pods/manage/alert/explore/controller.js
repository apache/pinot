/**
 * Controller for Alert Details Page: Overview Tab
 * @module manage/alert/explore
 * @exports manage/alert/explore
 */
import fetch from 'fetch';
import moment from 'moment';
import Controller from '@ember/controller';
import { checkStatus, buildDateEod, postProps } from 'thirdeye-frontend/helpers/utils';

export default Controller.extend({
  /**
   * Be ready to receive time span for anomalies via query params
   */
  queryParams: ['duration', 'startDate', 'endDate'],
  duration: null,
  startDate: null,
  endDate: null,

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
   * Mapping anomaly table column names to corresponding prop keys
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
      missingAnomalyProps: {},
      selectedSortMode: '',
      selectedTimeRange: '',
      selectedFilters: JSON.stringify({}),
      isAlertReady: false,
      openReportModal: false,
      isReplayStarted: true,
      isReplayPending: false,
      isReplayDone: false,
      isReportSuccess: false,
      isReportFailure: false,
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
   * Table pagination: number of pages to display
   * @type {Number}
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
   * Table pagination: total Number of pages to display
   * @type {Number}
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
   * Table pagination: creates the page Array for view
   * @type {Array}
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

      return [...new Array(size)].map((page, index) => startingNumber + index);
    }
  ),

  /**
   * Table pagination: pre-filtered and sorted anomalies with pagination
   * @type {Array}
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
      case 'days':
        return 1440;
      case 'hours':
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
   * date-time-picker: returns a time object from selected range end date
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
   * date-time-picker: returns a time object from selected range start date
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
   * date-time-picker: indicates the date format to be used based on granularity
   * @type {String}
   */
  uiDateFormat: Ember.computed('alertData.windowUnit', function() {
    const granularity = this.get('alertData.windowUnit').toLowerCase();

    switch(granularity) {
      case 'days':
        return 'MMM D, YYYY';
      case 'hours':
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
    'alertEvalMetrics',
    function() {
      const total = this.get('totalAnomalies') || 0;
      const evalObj = this.get('alertEvalMetrics.eval');
      const mttd = this.get('alertEvalMetrics.mttd') || '0';
      const projected = this.get('alertEvalMetrics.projected') || '0';
      const responseRate = evalObj && evalObj.responseRate ? evalObj.responseRate : '0';
      const precision = evalObj && evalObj.precision ? evalObj.precision : '0';
      const recall = evalObj && evalObj.recall ? evalObj.recall : '0';

      const anomalyStats = [
        {
          title: 'Number of anomalies',
          text: 'Estimated average number of anomalies per month',
          value: total,
          projected
        },
        {
          title: 'Response rate',
          text: 'Percentage of anomalies that has a response',
          value: responseRate
        },
        {
          title: 'Precision',
          text: 'Among all anomalies detected, the percentage of them that are true.',
          value: precision,
          projected
        },
        {
          title: 'Recall',
          text: 'Among all anomalies that happened, the percentage of them detected by the system',
          value: recall,
          projected
        },
        {
          title: 'MTTD for >30% change',
          text: 'Minimum time to detect for anomalies with > 30% change',
          value: mttd,
          projected
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
      const {
        selectedDimension: targetDimension,
        selectedResolution: targetResolution
      } = this.getProperties('selectedDimension', 'selectedResolution');
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
  renderDate(range) {
    // TODO: enable single day range
    const newDate = buildDateEod(range, 'days').format("DD MM YYY");
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
    const startTime = buildDateEod(1, 'month').format("YYYY-MM-DD");
    const endTime = buildDateEod(1, 'day').format("YYYY-MM-DD");
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

    // Two ways to trigger replay
    if (this.get('isReplayModeWrapper')) {
      fetch(replayWrapperUrl, postProps('')).then((res) => checkStatus(res, 'post')).then(() => {
        this.set('isReplayPending', false);
      }).catch(() => {
        this.set('isReplayStatusError', true);
      });
    } else {
      fetch(replayStartUrl, postProps('')).then((res) => checkStatus(res, 'post')).then((response) => {
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
    const data = { feedbackType, comment: '' };
    return fetch(url, postProps(data)).then((res) => checkStatus(res, 'post'));
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
   * Send a POST request to the report anomaly API (2-step process)
   * http://go/te-ss-alert-flow-api
   * @method reportAnomaly
   * @param {String} id - The anomaly id
   * @param {Object} data - The input values from 'report new anomaly' modal
   * @return {Promise}
   */
  reportAnomaly(id, data) {
    const reportUrl = `/anomalies/reportAnomaly/${id}`;
    const requiredProps = [data.startTime, data.endTime, data.feedbackType];
    const missingData = !requiredProps.every(prop => Ember.isPresent(prop));

    if (missingData) {
      return Promise.reject(new Error('missing data'));
    } else {
      data.startTime = moment(data.startTime).utc().valueOf();
      data.endTime = moment(data.endTime).utc().valueOf();
      return fetch(reportUrl, postProps(data)).then((res) => checkStatus(res, 'post'))
        .then((saveResult) => {
          const updateUrl = `/anomalies/updateFeedbackRange/${data.startTime}/${data.endTime}/${id}`;
          return fetch(updateUrl, postProps(data)).then((res) => checkStatus(res, 'post'));
        });
    }
  },

  /**
   * When exiting route, lets kill the replay status check calls
   * @method clearAll
   * @return {undefined}
   */
  clearAll() {
    this.set('requestCanContinue', false);
  },

  /**
   * Actions for alert page
   */
  actions: {

    /**
     * Handle selected dimension filter
     * @method onSelectDimension
     * @param {Object} selectedObj - the user-selected dimension to filter by
     */
    onSelectDimension(selectedObj) {
      this.set('selectedDimension', selectedObj);
    },

    /**
     * Handle selected resolution filter
     * @method onSelectResolution
     * @param {Object} selectedObj - the user-selected resolution to filter by
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
     */
    onChangeAnomalyResponse(anomalyRecord, selectedResponse, inputObj) {
      const responseObj = this.get('anomalyResponseObj').find(res => res.name === selectedResponse);
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
                return Promise.reject(new Error('Response not saved'));
              }
            }); // verifyAnomalyFeedback
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
     * Handle submission of missing anomaly form from alert-report-modal
     */
    onSave() {
      const { alertId, missingAnomalyProps } = this.getProperties('alertId', 'missingAnomalyProps');
      this.reportAnomaly(alertId, missingAnomalyProps)
        .then((result) => {
          this.setProperties({
            openReportModal: false,
            isReportSuccess: true
          });
        })
        // If failure, leave modal open and report
        .catch((err) => {
          this.setProperties({
            missingAnomalyProps: {},
            isReportFailure: true
          });
        });
    },

    /**
     * Handle missing anomaly modal cancel
     */
    onCancel() {
      this.setProperties({
        openReportModal: false,
        isReportSuccess: false,
        isReportFailure: false
      });
    },

    /**
     * Open modal for missing anomalies
     */
    onClickReportAnomaly() {
      this.set('openReportModal', true);
    },

    /**
     * Received bubbled-up action from modal
     * @param {Object} all input field values
     */
    onInputMissingAnomaly(inputObj) {
      this.set('missingAnomalyProps', inputObj);
    },

    /**
     * Handle display of selected baseline options
     * @param {Object} wowObj - the baseline selection
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
        anomalyData.forEach((anomaly) => {
          const wow = anomaly.wowData;
          const wowDetails = wow.compareResults.find(res => res.compareMode === wowObj.name);
          let curr = anomaly.current;
          let base = anomaly.baseline;
          let change = anomaly.changeRate;

          if (wowDetails) {
            curr = wow.currentVal.toFixed(2);
            base = wowDetails.baselineValue.toFixed(2);
            change = wowDetails.change.toFixed(2);
          }

          Ember.setProperties( anomaly, {
            shownCurrent: curr,
            shownBaseline: base,
            shownChangeRate: change
          });
        });
      }
    },

    /**
     * Handle display of selected anomaly time ranges (reload the model with new query params)
     * @method onRangeOptionClick
     * @param {Object} rangeOption - the selected range object
     */
    onRangeOptionClick(rangeOption) {
      const rangeFormat = 'YYYY-MM-DD';
      const defaultEndDate = buildDateEod(1, 'day').valueOf();

      // Trigger reload in model with new time range. Transition for 'custom' dates is handled by 'onRangeSelection'
      if (rangeOption.value !== 'custom') {
        this.transitionToRoute({ queryParams: {
          mode: 'explore',
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
     */
    onRangeSelection(start, end) {
      this.set('timeRangeOptions', this.newTimeRangeOptions('custom'));
      this.transitionToRoute({ queryParams: {
        mode: 'explore',
        duration: 'custom',
        startDate: moment(start).valueOf(),
        endDate: moment(end).valueOf()
      }});
    },

    /**
     * Load tuning sub-route
     */
    onClickTuneSensitivity() {
      this.transitionToRoute('manage.alert.tune', this.get('alertId'));
    },

    /**
     * Handle sorting for each sortable table column
     * @param {String} sortKey  - stringified start date
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

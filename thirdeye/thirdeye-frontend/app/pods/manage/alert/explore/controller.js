/**
 * Controller for Alert Details Page: Overview Tab
 * @module manage/alert/explore
 * @exports manage/alert/explore
 */
import _ from 'lodash';
import fetch from 'fetch';
import moment from 'moment';
import { set } from "@ember/object";
import { later } from "@ember/runloop";
import Controller from '@ember/controller';
import { computed } from '@ember/object';
import { isPresent } from "@ember/utils";
import { checkStatus, postProps, buildDateEod } from 'thirdeye-frontend/utils/utils';
import { buildAnomalyStats } from 'thirdeye-frontend/utils/manage-alert-utils';

export default Controller.extend({
  /**
   * Be ready to receive time span for anomalies via query params
   */
  queryParams: ['duration', 'startDate', 'endDate'],
  duration: null,
  startDate: null,
  endDate: null,

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
  initialize() {
    this.setProperties({
      filters: {},
      metricData: {},
      loadedWowData: [],
      predefinedRanges: {},
      missingAnomalyProps: {},
      selectedSortMode: '',
      replayErrorMailtoStr: '',
      selectedTimeRange: '',
      selectedFilters: JSON.stringify({}),
      openReportModal: false,
      isReplayStarted: true,
      isAlertReady: false,
      isReplayDone: false,
      isGraphReady: false,
      isReportSuccess: false,
      isReportFailure: false,
      isPageLoadFailure: false,
      isReplayModeWrapper: true,
      isReplayStatusError: false,
      isAnomalyArrayChanged: false,
      requestCanContinue: true,
      sortColumnStartUp: false,
      sortColumnScoreUp: false,
      sortColumnChangeUp: false,
      sortColumnResolutionUp: false,
      checkReplayInterval: 2000, // 2 seconds
      selectedDimension: 'All Dimensions',
      selectedResolution: 'All Resolutions',
      dateRangeToRender: [30, 10, 5],
      currentPage: 1,
      pageSize: 10
    });

    // Start checking for replay to end if a jobId is present
    if (this.get('isReplayPending')) {
      this.checkReplayStatus(this.get('jobId'));
    }
  },

  /**
   * Table pagination: number of pages to display
   * @type {Number}
   */
  paginationSize: computed(
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
  pagesNum: computed(
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
  viewPages: computed(
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
  paginatedFilteredAnomalies: computed(
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
  timePickerIncrement: computed('alertData.windowUnit', function() {
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
   * date-time-picker: returns a time object from selected range end date
   * @type {Object}
   */
  viewRegionEnd: computed(
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
  viewRegionStart: computed(
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
  uiDateFormat: computed('alertData.windowUnit', function() {
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
   * TODO: pull this into its own component, as we're re-using it in manage/alert/tune
   * @type {Object}
   */
  anomalyStats: computed(
    'alertEvalMetrics',
    'alertEvalMetrics.projected',
    function() {
      const evalMetrics = this.get('alertEvalMetrics');
      return buildAnomalyStats(evalMetrics, 'explore');
    }
  ),

  /**
   * If user selects a dimension from the dropdown, we filter the anomaly results here.
   * NOTE: this is currently set up to support single-dimension filters
   * @type {Object}
   */
  filteredAnomalies: computed(
    'selectedDimension',
    'selectedResolution',
    'anomalyData',
    'anomaliesLoaded',
    function() {
      const {
        anomaliesLoaded,
        selectedDimension: targetDimension,
        selectedResolution: targetResolution
      } = this.getProperties('selectedDimension', 'selectedResolution', 'anomaliesLoaded');
      let anomalies = [];

      if (anomaliesLoaded) {
        anomalies = this.get('anomalyData');
        if (targetDimension !== 'All Dimensions') {
          // Filter for selected dimension
          anomalies = anomalies.filter(data => targetDimension === data.dimensionString);
        }
        if (targetResolution !== 'All Resolutions') {
          // Filter for selected resolution
          anomalies = anomalies.filter(data => targetResolution === data.anomalyFeedback);
        }
      }
      return anomalies;
    }
  ),

  /**
   * Placeholder for options for range field. Here we generate arbitrary date ranges from our config.
   * @type {Array}
   */
  rangeOptionsExample: computed(
    'dateRangeToRender',
    function() {
      return this.get('dateRangeToRender').map(this.renderDate);
    }
  ),

  /**
   * Find the active baseline option name
   * @type {String}
   */
  baselineTitle: computed(
    'baselineOptions',
    function() {
      const activeOpName = this.get('baselineOptions').filter(item => item.isActive)[0].name;
      const displayName = `Current/${activeOpName}`;
      return displayName;
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
   * Pings the job-info endpoint to check status of an ongoing replay job.
   * If there is no progress after a set time, we display an error message.
   * TODO: Refactor method to use concurrency task instead of run.later
   * @method checkReplayStatus
   * @param {Number} jobId - the id for the newly triggered replay job
   * @return {fetch promise}
   */
  checkReplayStatus(jobId) {
    const checkStatusUrl = `/detection-onboard/get-status?jobId=${jobId}`;
    const {
      alertId,
      functionName,
      requestCanContinue,
      checkReplayInterval
    } = this.getProperties('alertId', 'functionName', 'requestCanContinue', 'checkReplayInterval');
    const br = `\r\n`;
    const replayStatusArr = ['completed', 'timeout'];
    const subject = 'TE Self-Serve Create Alert Issue';
    const intro = `TE Team, please look into a replay error for...${br}${br}`;
    const mailtoString = `mailto:ask_thirdeye@linkedin.com?subject=${encodeURIComponent(subject)}&body=`;

    // In replay status check, continue to display "pending" banner unless we have known success or failure.
    fetch(checkStatusUrl).then(checkStatus)
      .then((jobStatus) => {
        const replayStatusObj = _.has(jobStatus, 'taskStatuses')
          ? jobStatus.taskStatuses.find(status => status.taskName === 'FunctionReplay')
          : null;
        const replayErr = replayStatusObj ? replayStatusObj.message : 'N/A';
        const replayStatus = replayStatusObj ? replayStatusObj.taskStatus.toLowerCase() : '';
        const bodyString = `${intro}jobId: ${jobId}${br}alertId: ${alertId}${br}functionName: ${functionName}${br}${br}error: ${replayErr}`;

        if (replayStatusArr.includes(replayStatus)) {
          this.set('isReplayPending', false);
          this.send('refreshModel');
          this.transitionToRoute('manage.alert', alertId, { queryParams: { jobId: null }});
        } else if (replayStatus === 'failed') {
          this.setProperties({
            isReplayStatusError: true,
            replayErrorMailtoStr: mailtoString + encodeURIComponent(bodyString)
          });
        } else if (requestCanContinue) {
          later(() => {
            this.checkReplayStatus(jobId);
          }, checkReplayInterval);
        }
      });
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
   * @param {String} id - The alert id
   * @param {Object} data - The input values from 'report new anomaly' modal
   * @return {Promise}
   */
  reportAnomaly(id, data) {
    const reportUrl = `/anomalies/reportAnomaly/${id}?`;
    const updateUrl = `/anomalies/updateFeedbackRange/${data.startTime}/${data.endTime}/${id}?feedbackType=${data.feedbackType}`;
    const requiredProps = ['data.startTime', 'data.endTime', 'data.feedbackType'];
    const missingData = !requiredProps.every(prop => isPresent(prop));
    let queryStringUrl = reportUrl;

    if (missingData) {
      return Promise.reject(new Error('missing data'));
    } else {
      Object.entries(data).forEach(([key, value]) => {
        queryStringUrl += `&${encodeURIComponent(key)}=${encodeURIComponent(value)}`;
      });
      // Step 1: Report the anomaly
      return fetch(queryStringUrl, postProps('')).then((res) => checkStatus(res, 'post'))
        .then((saveResult) => {
          // Step 2: Automatically update anomaly feedback in that range
          return fetch(updateUrl, postProps('')).then((res) => checkStatus(res, 'post'));
        });
    }
  },

  /**
   * When exiting route, lets kill the replay status check calls
   * @method clearAll
   * @return {undefined}
   */
  clearAll() {
    this.setProperties({
      requestCanContinue: false,
      alertEvalMetrics: {}
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
      set(inputObj, 'selected', selectedResponse);

      // Save anomaly feedback
      this.updateAnomalyFeedback(anomalyRecord.anomalyId, responseObj.value)
        .then((res) => {
          // We make a call to ensure our new response got saved
          this.verifyAnomalyFeedback(anomalyRecord.anomalyId, responseObj.status)
            .then((res) => {
              const filterMap = res.searchFilters ? res.searchFilters.statusFilterMap : null;
              if (filterMap && filterMap.hasOwnProperty(responseObj.status)) {
                set(anomalyRecord, 'anomalyFeedback', selectedResponse);
                set(anomalyRecord, 'showResponseSaved', true);
              } else {
                return Promise.reject(new Error('Response not saved'));
              }
            }); // verifyAnomalyFeedback
        }) // updateAnomalyFeedback
        .catch((err) => {
          set(anomalyRecord, 'showResponseFailed', true);
          set(anomalyRecord, 'showResponseSaved', false);
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
          this.set('isReportSuccess', true);
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
        isReportSuccess: false,
        isReportFailure: false,
        renderModalContent: false
      });
    },

    /**
     * Open modal for missing anomalies
     */
    onClickReportAnomaly() {
      this.setProperties({
        isReportSuccess: false,
        isReportFailure: false,
        openReportModal: true
      });
      // We need the C3/D3 graph to render after its containing parent elements are rendered
      // in order to avoid strange overflow effects.
      later(() => {
        this.set('renderModalContent', true);
      });
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
          const wowDetails = wow.compareResults.find(res => res.compareMode.toLowerCase() === wowObj.name.toLowerCase());
          let curr = anomaly.current;
          let base = anomaly.baseline;
          let change = anomaly.changeRate;

          if (wowDetails) {
            curr = wow.currentVal.toFixed(2);
            base = wowDetails.baselineValue.toFixed(2);
            change = wowDetails.change.toFixed(2);
          }

          this.setProperties(anomaly, {
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

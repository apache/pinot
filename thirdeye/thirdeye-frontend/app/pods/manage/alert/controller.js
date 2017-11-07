/**
 * Controller for Alert Landing and Details Page
 * @module manage/alert
 * @exports manage/alert
 */
import fetch from 'fetch';
import Ember from 'ember';
import moment from 'moment';
import { checkStatus } from 'thirdeye-frontend/helpers/utils';
import { task, timeout } from 'ember-concurrency';

export default Ember.Controller.extend({
  /**
   * Set up to receive prompt to trigger page mode change.
   * When replay is received as true, it indicates that this is a
   * newly created alert and replay is needed in order to display anomaly data.
   */
  queryParams: ['replay'],
  replay: false,

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
   * Set initial view values
   * @method initialize
   * @param {Boolean} isReplayNeeded
   * @return {undefined}
   */
  initialize(isReplayNeeded) {
    this.setProperties({
      filters: {},
      graphConfig: {},
      selectedFilters: JSON.stringify({}),
      isAlertReady: false,
      isReplayStarted: true,
      isReplayPending: false,
      isReplayDone: false,
      isReplayStatusError: false,
      isReplayModeWrapper: true,
      requestCanContinue: true,
      selectedDimension: 'All Dimensions',
      dateRangeToRender: [30, 10, 5],
    });
    // Toggle page mode if replay is needed
    if (isReplayNeeded) {
      this.set('isReplayPending', true);
      this.triggerReplay(this.get('alertId'));
    }
  },

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
   * Array containing the types of anomaly responses supported
   * @type {Array}
   */
  responseOptions: ['Not reviewed yet', 'True anomaly', 'False alarm', 'I don\'t know', 'Confirmed - New Trend'],

  /**
   * Data needed to render the stats 'cards' above the anomaly graph for this alert
   * @type {Object}
   */
  anomalyStats: Ember.computed(
    'totalAnomalies',
    function() {
      const total = this.get('totalAnomalies');
      const anomalyStats = [
        {
          title: 'Number of anomalies',
          text: 'Estimated average number of anomalies per month',
          value: total,
        },
        {
          title: 'Response rate',
          text: 'Percentage of anomalies that has a response',
          value: '87.1%'
        },
        {
          title: 'Precision',
          text: 'Among all anomalies detected, the percentage of them that are true.',
          value: '50%'
        },
        {
          title: 'Recall',
          text: 'Among all anomalies that happened, the percentage of them detected by the system',
          value: '25%'
        },
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
    'anomalyData',
    function() {
      const targetDimension = this.get('selectedDimension');
      let anomalies = this.get('anomalyData');

      if (targetDimension === 'All Dimensions') {
        return anomalies;
      } else {
        return anomalies.filter(data => {
          return targetDimension === `${data.dimensionList[0].dimensionKey}:${data.dimensionList[0].dimensionVal}`;
        });
      }
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
      let now = moment();
      let isTimeUp = (now - timerStart) > 165;
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
   * Sends a request to begin advanced replay for a metric. The replay will fetch new
   * time-series data based on user-selected sensitivity settings.
   * @method triggerReplay
   * @param {Number} functionId - the id for the newly created function (alert)
   * @return {Ember.RSVP.Promise}
   */
  triggerReplay(functionId) {
    const anomalies = this.get('anomalyData');
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
    const sendReportUrl = `/thirdeye/email/generate/functions/${startTime}/${endTime}?functions=${functionId}` +
      `&subject=${subject}to=${recipients}&includeSentAnomaliesOnly=true&isApplyFilter=true`;
    const postProps = {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      }
    };

    // Two ways to trigger replay
    if (isReplayModeWrapper) {
      fetch(replayWrapperUrl, postProps).then((res) => checkStatus(res, 'post')).then((response) => {
        this.set('isReplayPending', false);
      })
      .catch((error) => {
        this.set('isReplayStatusError', true);
      });
    } else {
      fetch(replayStartUrl, postProps).then((res) => checkStatus(res, 'post')).then((response) => {
        response.json().then((jobId) => {
          this.checkReplayStatus(Object.values(jobId)[0]);
        });
      })
      .catch((error) => {
        this.set('isReplayStatusError', true);
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
      requestCanContinue: false
    });
  },

  /**
   * Actions for alert page
   */
  actions: {

    /**
     * When exiting route, lets kill the replay status check calls
     * @method onSelectDimension
     * @param {Object} selectedObj - the user-selected dimension to filter by
     * @return {undefined}
     */
    onSelectDimension(selectedObj) {
      this.setProperties({
        selectedDimension: selectedObj
      });
    }
  }
});

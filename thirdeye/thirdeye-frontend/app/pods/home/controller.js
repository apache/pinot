import Controller from '@ember/controller';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import { computed, set } from '@ember/object';
import moment from 'moment';
import { setUpTimeRangeOptions } from 'thirdeye-frontend/utils/manage-alert-utils';
import * as anomalyUtil from 'thirdeye-frontend/utils/anomaly';
import _ from 'lodash';

const TIME_PICKER_INCREMENT = 5; // tells date picker hours field how granularly to display time
const DEFAULT_ACTIVE_DURATION = '1d'; // setting this date range selection as default (Last 24 Hours)
const UI_DATE_FORMAT = 'MMM D, YYYY hh:mm a'; // format for date picker to use (usually varies by route or metric)
const DISPLAY_DATE_FORMAT = 'YYYY-MM-DD HH:mm'; // format used consistently across app to display custom date range
const TIME_RANGE_OPTIONS = ['today', '1d', '2d', '1w'];

export default Controller.extend({
  queryParams: ['appName', 'startDate', 'endDate', 'duration'],
  appName: null,
  startDate: null,
  endDate: null,
  duration: null,
  anomalyResponseObj: anomalyUtil.anomalyResponseObj,
  feedbackType: 'All Resolutions',

  /**
   * Overrides ember-models-table's css classes
   */
  classes: {
    table: 'table table-striped table-bordered table-condensed'
  },

  init() {
    this._super(...arguments);
    this.get('anomalyResponseObj').push({
      name: 'All Resolutions',
      value: 'ALL',
      status: 'All Resolutions'
    });
  },

  anomalyResponseNames: computed(
    'anomalyResponseObj',
    function() {
      return anomalyUtil.anomalyResponseObj.mapBy('name');
    }
  ),
  filteredAnomalyMapping: computed(
    'model.anomalyMapping', 'feedbackType',
    function() {
      let filteredAnomalyMapping = this.get('model.anomalyMapping');
      const feedbackType = this.get('feedbackType');
      const feedbackItem = this._checkFeedback(feedbackType);

      if (feedbackItem.value !== 'ALL' && !_.isEmpty(filteredAnomalyMapping)) {
        let map = {};
          // Iterate through each anomaly
        Object.keys(filteredAnomalyMapping).some(function(key) {
            filteredAnomalyMapping[key].forEach(attr => {
              if (attr.anomaly.data.feedback === feedbackItem.value) {
                if (!map[key]) {
                  map[key] = [];
                }
                map[key].push(attr);
              }
            });
        });
        return map;
      } else {
        return filteredAnomalyMapping;
      }

    }
  ),

  /**
   * Date types to display in the pills
   * @type {Object[]} - array of objects, each of which represents each date pill
   */
  pill: computed(
    'model.{appName,startDate,endDate,duration}',
    function() {
      const appName = this.get('model.appName');
      const startDate = Number(this.get('model.startDate'));
      const endDate = Number(this.get('model.endDate'));
      const duration = this.get('model.duration') || DEFAULT_ACTIVE_DURATION;
      const predefinedRanges = {
        'Today': [moment().startOf('day'), moment()],
        'Last 24 hours': [moment().subtract(1, 'day'), moment()],
        'Yesterday': [moment().subtract(1, 'day').startOf('day'), moment().subtract(1, 'days').endOf('day')],
        'Last Week': [moment().subtract(1, 'week'), moment()]
      };

      return {
        appName,
        uiDateFormat: UI_DATE_FORMAT,
        activeRangeStart: moment(startDate).format(DISPLAY_DATE_FORMAT),
        activeRangeEnd: moment(endDate).format(DISPLAY_DATE_FORMAT),
        timeRangeOptions: setUpTimeRangeOptions(TIME_RANGE_OPTIONS, duration),
        timePickerIncrement: TIME_PICKER_INCREMENT,
        predefinedRanges
      };
    }
  ),

  /**
   * Stats to display in cards
   * @type {Object[]} - array of objects, each of which represents a stats card
   */
  stats: computed(
    'model.anomalyPerformance',
    function() {
      if (!this.get('model.anomalyPerformance')) {
        return {};
      }

      const { responseRate, precision, recall } = this.get('model.anomalyPerformance').getProperties('responseRate', 'precision', 'recall');
      const totalAlertsDescription = 'Total number of anomalies that occured over a period of time';
      const responseRateDescription = '% of anomalies that are reviewed';
      const precisionDescription = '% of all anomalies detected by the system that are true';
      const recallDescription = '% of all anomalies detected by the system';
      //TODO: Since totalAlerts is not correct here. We will use anomaliesCount for now till backend api is fixed. - lohuynh
      const statsArray = [
        ['Number of anomalies', totalAlertsDescription, this.get('anomaliesCount'), 'digit'],
        ['Response Rate', responseRateDescription, floatToPercent(responseRate), 'percent'],
        ['Precision', precisionDescription, floatToPercent(precision), 'percent'],
        ['Recall', recallDescription, floatToPercent(recall), 'percent']
      ];

      return statsArray;
    }
  ),

  /**
   * Helper for getting the matching selected response feedback object
   * @param {string} selected - selected filter by value
   * @return {string}
   */
  _checkFeedback: function(selected) {
    return this.get('anomalyResponseObj').find((type) => {
       return type.name === selected;
     });
  },

  actions: {
    /**
     * Sets the selected application property based on user selection
     * @param {Object} selectedApplication - object that represents selected application
     * @return {undefined}
     */
    selectApplication(selectedApplication) {
      set(this, 'appName', selectedApplication.get('application'));
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
      const appName = this.get('appName');
      //Update the time range option selected
      this.set('timeRangeOptions', setUpTimeRangeOptions(TIME_RANGE_OPTIONS, duration));
      this.transitionToRoute({ queryParams: { appName, duration, startDate, endDate }});
    },

    /**
     * Handle dynamically saving anomaly feedback responses
     * @method onFilterBy
     * @param {String} feedbackType - the current feedback type
     * @param {String} selected - the selection item
     */
     onFilterBy(feedbackType, selected) {
       const feedbackItem = this._checkFeedback(selected);
       this.set('feedbackType', feedbackItem.name);
    }
  }
});

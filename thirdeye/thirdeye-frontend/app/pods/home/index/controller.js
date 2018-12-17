import Controller from '@ember/controller';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import { computed, set, get, setProperties } from '@ember/object';
import { isBlank } from '@ember/utils';
import moment from 'moment';
import _ from 'lodash';
import { setUpTimeRangeOptions } from 'thirdeye-frontend/utils/manage-alert-utils';
import * as anomalyUtil from 'thirdeye-frontend/utils/anomaly';
import { inject as service } from '@ember/service';

const TIME_PICKER_INCREMENT = 5; // tells date picker hours field how granularly to display time
const DEFAULT_ACTIVE_DURATION = '1d'; // setting this date range selection as default (Last 24 Hours)
const UI_DATE_FORMAT = 'MMM D, YYYY hh:mm a'; // format for date picker to use (usually varies by route or metric)
const DISPLAY_DATE_FORMAT = 'YYYY-MM-DD HH:mm'; // format used consistently across app to display custom date range
const TIME_RANGE_OPTIONS = ['today', '1d', '2d', '1w'];

export default Controller.extend({
  toggleCollapsed: false, /* hide/show accordians */
  isReportAnomalyEnabled: false,
  store: service('store'),

  /**
   * Overrides ember-models-table's css classes
   */
  classes: {
    table: 'table table-striped table-bordered table-condensed'
  },

  init() {
    this._super(...arguments);
    // Add ALL option to copy of global anomaly response object
    const anomalyResponseFilterTypes = _.cloneDeep(anomalyUtil.anomalyResponseObj);
    anomalyResponseFilterTypes.push({
      name: 'All Resolutions',
      value: 'ALL',
      status: 'All Resolutions'
    });
    setProperties(this, {
      anomalyResponseFilterTypes,
      anomalyResponseNames: anomalyResponseFilterTypes.mapBy('name')
    });
  },

  sortedApplications: computed(
    'model.applications',
    function() {
      let model = get(this, 'model');

      // Iterate through each anomaly
      let applications =  this.get('store').peekAll('application').sortBy('application');
      return applications;
    }
  ),

  filteredAnomalyMapping: computed(
    'model.{anomalyMapping,feedbackType}',
    function() {
      let filteredAnomalyMapping = get(this, 'model.anomalyMapping');
      const feedbackType = get(this, 'model.feedbackType');
      const feedbackItem = this._checkFeedback(feedbackType);

      if (feedbackItem.value !== 'ALL' && !isBlank(filteredAnomalyMapping)) {
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
      const appName = get(this, 'model.appName');
      const startDate = Number(get(this, 'model.startDate'));
      const endDate = Number(get(this, 'model.endDate'));
      const duration = get(this, 'model.duration') || DEFAULT_ACTIVE_DURATION;
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
    'model.anomalyMapping',
    function() {
      const anomalyMapping = get(this, 'model.anomalyMapping');
      if (!anomalyMapping) {
        return {};
      }
      let respondedAnomaliesCount = 0;
      let truePositives = 0;
      let falsePositives = 0;
      let falseNegatives = 0;
      Object.keys(anomalyMapping).forEach(function (key) {
        anomalyMapping[key].forEach(function (attr) {
          const classification = attr.anomaly.data.classification;
          if (classification != 'NONE') {
            respondedAnomaliesCount++;
            if (classification == 'TRUE_POSITIVE') {
              truePositives++;
            } else if (classification == 'FALSE_POSITIVE') {
              falsePositives++;
            } else if (classification == 'FALSE_NEGATIVE') {
              falseNegatives++;
            }
          }
        });
      });

      const totalAnomaliesCount = get(this, 'anomaliesCount');
      const responseRate = respondedAnomaliesCount / totalAnomaliesCount;
      const precision = truePositives / (truePositives + falsePositives);
      const recall = truePositives / (truePositives + falseNegatives);
      const totalAlertsDescription = 'Total number of anomalies that occured over a period of time';
      const responseRateDescription = '% of anomalies that are reviewed';
      const precisionDescription = '% of all anomalies detected by the system that are true';
      const recallDescription = '% of all anomalies detected by the system';
      //TODO: Since totalAlerts is not correct here. We will use anomaliesCount for now till backend api is fixed. - lohuynh
      const statsArray = [
        ['Number of anomalies', totalAlertsDescription, totalAnomaliesCount, 'digit'],
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
    return get(this, 'anomalyResponseFilterTypes').find((type) => {
      return type.name === selected;
    });
  },

  actions: {

    /**
     * Sets the selected metric alert if user triggers power-select
     * @param {String} metric - the metric group for the selection
     * @param {String} alertNeme - name of selected alert
     * @return {undefined}
     */
    onSelectAlert(metric, alertName) {
      const targetMetricRecord = get(this.model, 'alertsByMetric')[metric];
      if (targetMetricRecord.names.length > 1) {
        targetMetricRecord.selectedIndex = targetMetricRecord.names.findIndex(alert => { return alert === alertName; });
      }
    },

    /**
     * Navigates to the alert page for selected alert for the purpose of reporting a missing anomaly
     * @param {String} metric - the metric group for the selection
     * @param {Array} anomalyList - array of anomalies for selected metric
     * @return {undefined}
     */
    onClickReport(metric, anomalyList) {
      const targetMetricRecord = get(this.model, 'alertsByMetric')[metric];
      const targetId = targetMetricRecord.ids[targetMetricRecord.selectedIndex];
      const duration = anomalyList[0].humanizedObject.queryDuration;
      const startDate = anomalyList[0].humanizedObject.queryStart;
      const endDate = anomalyList[0].humanizedObject.queryEnd;
      // Navigate to alert page for selected alert
      if (targetId) {
        this.transitionToRoute('manage.alert', targetId, { queryParams: {
          duration,
          startDate,
          endDate,
          openReport: true
        }});
      }
    },

    /**
     * Toggles the show/hide of the metric tables
     */
    toggleAllAccordions() {
      this.toggleProperty('toggleCollapsed');
    },

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
      const appName = get(this, 'appName');
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
      set(this, 'feedbackType', feedbackItem.name);
    }
  }
});

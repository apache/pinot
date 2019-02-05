/**
 * This component displays the alert details. It would be used in places like Alert Details, and Preview pages/modules.
 * @module components/alert-details
 * @property {Object} alertYaml - the alert yaml
 * @property {boolean} disableYamlSave  - detect flag for yaml changes
 * @example
   {{#alert-details
     alertYaml=alertYaml
     disableYamlSave=disableYamlSave
   }}
     {{yield}}
   {{/alert-details}}
 * @exports alert-details
 */

import Component from '@ember/component';
import { computed, observer, setProperties, set, get } from '@ember/object';
import { inject as service } from '@ember/service';
import { task } from 'ember-concurrency';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import { setUpTimeRangeOptions } from 'thirdeye-frontend/utils/manage-alert-utils';
import moment from 'moment';

const TIME_PICKER_INCREMENT = 5; // tells date picker hours field how granularly to display time
const DEFAULT_ACTIVE_DURATION = '1m'; // setting this date range selection as default (Last 24 Hours)
const UI_DATE_FORMAT = 'MMM D, YYYY hh:mm a'; // format for date picker to use (usually varies by route or metric)
const DISPLAY_DATE_FORMAT = 'YYYY-MM-DD HH:mm'; // format used consistently across app to display custom date range
const TIME_RANGE_OPTIONS = ['1m', '3m'];

export default Component.extend({
  anomaliesApiService: service('services/api/anomalies'),
  notifications: service('toast'),
  anomalyMapping: {},
  analysisRange: [moment().subtract(1, 'month').startOf('hour').valueOf(), moment().startOf('hour').valueOf()],
  displayRange: [moment().subtract(2, 'month').startOf('hour').valueOf(), moment().startOf('hour').valueOf()],
  isPendingData: false,

  alertYamlChanged: observer('alertYaml', 'analysisRange', 'disableYamlSave', async function() {
    set(this, 'isPendingData', true);
    // deal with the change
    const alertYaml = get(this, 'alertYaml');
    if(alertYaml) {
      try {
        const anomalyMapping = await this.get('_getAnomalyMapping').perform(alertYaml);
        set(this, 'isPendingData', false);
        set(this, 'anomalyMapping', anomalyMapping);
      } catch (error) {
        throw new Error(`Unable to retrieve anomaly data. ${error}`);
      }
    }
  }),

  /**
   * Stats to display in cards
   * @type {Object[]} - array of objects, each of which represents a stats card
   */
  stats: computed(
    'anomalyMapping',
    function() {
      const anomalyMapping = get(this, 'anomalyMapping');
      if (!anomalyMapping) {
        return {};
      }
      let respondedAnomaliesCount = 0;
      let truePositives = 0;
      let falsePositives = 0;
      let falseNegatives = 0;
      let numberOfAnomalies = 0;

      Object.keys(anomalyMapping).forEach(function (key) {
        anomalyMapping[key].forEach(function (attr) {
          numberOfAnomalies++;
          if(attr.anomaly && attr.anomaly.data) {
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
          }
        });
      });

      const totalAnomaliesCount = numberOfAnomalies;
      const totalAlertsDescription = 'Total number of anomalies that occured over a period of time';
      let statsArray = [];
      if(respondedAnomaliesCount > 0) {
        const responseRate = respondedAnomaliesCount / totalAnomaliesCount;
        const precision = truePositives / (truePositives + falsePositives);
        const recall = truePositives / (truePositives + falseNegatives);
        const responseRateDescription = '% of anomalies that are reviewed';
        const precisionDescription = '% of all anomalies detected by the system that are true';
        const recallDescription = '% of all anomalies detected by the system';
        statsArray = [
          ['Anomalies', totalAlertsDescription, totalAnomaliesCount, 'digit'],
          ['Response Rate', responseRateDescription, floatToPercent(responseRate), 'percent'],
          ['Precision', precisionDescription, floatToPercent(precision), 'percent'],
          ['Recall', recallDescription, floatToPercent(recall), 'percent']
        ];
      } else {
        statsArray = [
          ['Anomalies', totalAlertsDescription, totalAnomaliesCount, 'digit']
        ];
      }
      return statsArray;
    }
  ),

  /**
   * Date types to display in the pills
   * @type {Object[]} - array of objects, each of which represents each date pill
   */
  pill: computed(
    'analysisRange', 'startDate', 'endDate' ,'duration',
    function() {
      const analysisRange = get(this, 'analysisRange');
      const startDate = Number(analysisRange[0]) || Number(get(this, 'startDate'));
      const endDate = Number(analysisRange[1]) || Number(get(this, 'endDate'));
      const duration = get(this, 'duration') || DEFAULT_ACTIVE_DURATION;
      const predefinedRanges = {
        'Today': [moment().startOf('day'), moment()],
        'Last 24 hours': [moment().subtract(1, 'day'), moment()],
        'Yesterday': [moment().subtract(1, 'day').startOf('day'), moment().subtract(1, 'days').endOf('day')],
        'Last Week': [moment().subtract(1, 'week'), moment()]
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

  _getAnomalyMapping: task (function * (alertYaml) {//TODO: need to add to anomaly util - LH
    let anomalyMapping = {};
    const analysisRange = get(this, 'analysisRange');
    const postProps = {
      method: 'POST',
      body: alertYaml,
      headers: { 'content-type': 'text/plain' }
    };
    const notifications = get(this, 'notifications');

    //detection alert fetch
    const start = analysisRange[0] || '1548489600000';
    const end = analysisRange[1] || '1548748800000';
    const alertUrl = `yaml/preview?start=${start}&end=${end}&tuningStart=0&tuningEnd=0`;
    try {
      const alert_result = yield fetch(alertUrl, postProps);
      const alert_status  = get(alert_result, 'status');
      const applicationAnomalies = yield alert_result.json();

      if (alert_status !== 200 && applicationAnomalies.message) {
        notifications.error(applicationAnomalies.message, 'Preview alert failed');
      } else {
        const anomalies = applicationAnomalies.anomalies;
        if (anomalies && anomalies.length > 0) {
          const humanizedObject = {
            queryDuration: '1m',
            queryStart: start,
            queryEnd: end
          };
          this.set('applicationAnomalies', anomalies);

          anomalies.forEach(anomaly => {
            const metricName = anomaly.metric;
            //Grouping the anomalies of the same metric name
            if (!anomalyMapping[metricName]) {
              anomalyMapping[metricName] = [];
            }

            // Group anomalies by metricName and function name (alertName) and wrap it into the Humanized cache. Each `anomaly` is the raw data from ember data cache.
            anomalyMapping[metricName].push(this.get('anomaliesApiService').getHumanizedEntity(anomaly, humanizedObject));
          });
        }
      }
    } catch (error) {
      notifications.error('Preview alert failed', error);
    }

    return anomalyMapping;
  }).drop(),

  actions: {
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
      set(this, 'analysisRange', [startDate, endDate]);
      set(this, 'duration', duration)
    },

    refreshPreview(){
      set(this, 'disableYamlSave', true);
    }
  }
});

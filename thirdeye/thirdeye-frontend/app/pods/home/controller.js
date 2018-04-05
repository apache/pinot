import Controller from '@ember/controller';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import { computed, get, set } from '@ember/object';
import moment from 'moment';
import { setUpTimeRangeOptions, setDuration } from 'thirdeye-frontend/utils/manage-alert-utils';

const PILL_CLASS = '.range-pill-selectors';
const START_DATE = 1513151999999; // arbitrary start date in milliseconds
const END_DATE = 1520873345292; // arbitrary end date in milliseconds
const TIME_PICKER_INCREMENT = 5; // tells date picker hours field how granularly to display time
const ACTIVE_SELECTION = '1d'; // setting this date range selection as default
const UI_DATE_FORMAT = 'MMM D, YYYY hh:mm a'; // format for date picker to use (usually varies by route or metric)
const DISPLAY_DATE_FORMAT = 'YYYY-MM-DD HH:mm'; // format used consistently across app to display custom date range
const TODAY = moment().startOf('day').add(1, 'days');
const TWO_WEEKS_AGO = moment().subtract(13, 'days').startOf('day');
const PRESET_RANGES = {
  'Today': [moment(), TODAY],
  'Last 2 weeks': [TWO_WEEKS_AGO, TODAY]
};
const TIME_RANGE_OPTIONS = ['1d', '2d', '1w'];

export default Controller.extend({
  queryParams: ['appName', 'startDate', 'endDate'],
  appName: null,
  startDate: null,
  endDate: null,

  /**
   * Overrides ember-models-table's css classes
   */
  classes: {
    table: 'table-bordered table-condensed te-anomaly-table--no-margin'
  },

  uiDateFormat: UI_DATE_FORMAT,
  activeRangeEnd: moment(END_DATE).format(DISPLAY_DATE_FORMAT),
  activeRangeStart: moment(START_DATE).format(DISPLAY_DATE_FORMAT),
  timeRangeOptions: setUpTimeRangeOptions(TIME_RANGE_OPTIONS, ACTIVE_SELECTION),
  timePickerIncrement: TIME_PICKER_INCREMENT,
  predefinedRanges: PRESET_RANGES,

  /**
   * Stats to display in cards
   * @type {Object[]} - array of objects, each of which represents a stats card
   */
  stats: computed(
    'model.anomalyPerformance',
    function() {
      const { totalAlerts, responseRate, precision, recall } = get(this, 'model.anomalyPerformance');
      const totalAlertsDescription = 'Total number of anomalies that occured over a period of time';
      const responseRateDescription = '% of anomalies that are reviewed';
      const precisionDescription = '% of all anomalies detected by the system that are true';
      const recallDescription = '% of all anomalies detected by the system';
      const statsArray = [
        ['Number of anomalies', totalAlertsDescription, totalAlerts, 'digit'],
        ['Response Rate', responseRateDescription, floatToPercent(responseRate), 'percent'],
        ['Precision', precisionDescription, floatToPercent(precision), 'percent'],
        ['Recall', recallDescription, floatToPercent(recall), 'percent']
      ];

      return statsArray;
    }
  ),

  actions: {
    /**
     * Sets the selected application property based on user selection
     * @param {Object} selectedApplication - object that represents selected application
     * @return {undefined}
     */
    selectApplication(selectedApplication) {
      set(this, 'appName', selectedApplication.application);
    },

    /**
     * Sets the new custom date range for anomaly coverage
     * @method onRangeSelection
     * @param {Object} rangeOption - the user-selected time range to load
     */
    onRangeSelection(rangeOption) {
      const {
        start,
        end,
        value: duration
      } = rangeOption;
      const startDate = moment(start).valueOf();
      const endDate = moment(end).valueOf();
      const appName = this.get('appName');
      //Update the time range option selected
      this.set('timeRangeOptions', setUpTimeRangeOptions(TIME_RANGE_OPTIONS, duration));
      this.transitionToRoute({ queryParams: { appName, duration, startDate, endDate }});
    },
  }
});

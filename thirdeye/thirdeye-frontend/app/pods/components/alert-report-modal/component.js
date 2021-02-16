/**
 * Component for "report missing anomaly" modal
 * @module components/alert-report-modal
 * @property {String} metricName - text for read-only metric field
 * @property {String} alertName  - text for read-only alert field
 * @property {Number} timePickerIncrement - config for time-range-picker
 * @property {String} viewRegionStart - range start timestamp
 * @property {String} viewRegionEnd - range end timestamp
 * @property {Object} predefinedRanges - needed for time-range-picker
 * @property {String} uiDateFormat - date format desired for time-range-picker
 * @example
  {{alert-report-modal
    metricName="mobile_notification_errors"
    alertName="notification_sessions_mobile"
    selectedDimension='dimension'
    alertHasDimensions=true
    timePickerIncrement=200
    viewRegionStart="2017-10-12 23:59"
    viewRegionEnd="2017-12-11 23:59"
    predefinedRanges=predefinedRanges
    uiDateFormat="MMM D, YYYY hh:mm a"
    inputAction=(action "onInputMissingAnomaly")
  }}
 * @exports alert-report-modal
 * @author smcclung
 */

import moment from 'moment';
import Component from '@ember/component';
import { computed } from '@ember/object';

export default Component.extend({
  containerClassNames: 'alert-report-modal',
  isNewTrend: false,
  showTimePicker: true,
  timePickerIncrement: 5,

  /**
   * Collects all input data for the post request
   * @method reportAnomalyPayload
   * @return {Object} Post data
   */
  reportAnomalyPayload: computed(
    'isNewTrend',
    'anomalyComments',
    'selectedDimension',
    'viewAnomalyStart',
    'viewAnomalyEnd',
    'anomalyLinks',
    function () {
      const postObj = {
        startTime: moment(this.get('viewAnomalyStart')).utc().valueOf(),
        endTime: moment(this.get('viewAnomalyEnd')).utc().valueOf(),
        feedbackType: this.get('isNewTrend') ? 'ANOMALY_NEW_TREND' : 'ANOMALY',
        dimension: this.get('showDimension') ? this.get('selectedDimension') || null : null,
        comment: this.get('anomalyComments') || null,
        externalURL: this.get('anomalyLinks') || null
      };

      return postObj;
    }
  ),

  /**
   * Collects all input data for the post request
   * @method reportAnomalyPayload
   * @return {Object} Post data
   */
  showDimension: computed('alertHasDimensions', 'selectedDimension', function () {
    const { alertHasDimensions, selectedDimension } = this.getProperties('alertHasDimensions', 'selectedDimension');

    return alertHasDimensions && selectedDimension !== 'Choose a dimension';
  }),

  /**
   * Sends post object as is to parent
   * @method bubbleModalInput
   */
  bubbleModalInput() {
    this.get('inputAction')(this.get('reportAnomalyPayload'));
  },

  actions: {
    /**
     * Handle selected dimension filter
     * @method onSelectDimension
     * @param {Object} selectedObj - the user-selected dimension to filter by
     */
    onSelectDimension(selectedObj) {
      this.set('selectedDimension', selectedObj);
      this.bubbleModalInput();
    },

    /**
     * Handle selected feedback type
     * @method onFeedbackTypeSelection
     * @param {Object} selectedObj - the user-selected feedback type
     */
    onFeedbackTypeSelection(trendSelection) {
      this.set('isNewTrend', trendSelection);
      this.bubbleModalInput();
    },

    /**
     * Handle changes to anomaly input
     * @method onAnomalyInput
     */
    onAnomalyInput() {
      this.bubbleModalInput();
    }
  }
});

/**
 * Component for "report missing anomaly" modal
 * @module components/alert-report-modal
 * @property {String} metricName - text for read-only metric field
 * @property {String} alertName  - text for read-only alert field
 * @property {Array} dimensionOptions - options for dimension select field
 * @property {Number} timePickerIncrement - config for time-range-picker
 * @property {String} maxTime - timestamp for loading anomaly graph
 * @property {String} viewRegionStart - range start timestamp
 * @property {String} viewRegionEnd - range end timestamp
 * @property {Object} predefinedRanges - needed for time-range-picker
 * @property {String} uiDateFormat - date format desired for time-range-picker
 * @property {String} graphMessageText - text for graph in loading state
 * @example
  {{alert-report-modal
    metricName="mobile_notification_errors"
    alertName="notification_sessions_mobile"
    dimensionOptions=['dimension 1', 'dimension 2']
    timePickerIncrement=200
    maxTime="1513137100914"
    viewRegionStart="2017-10-12 23:59"
    viewRegionEnd="2017-12-11 23:59"
    predefinedRanges=predefinedRanges
    uiDateFormat="MMM D, YYYY hh:mm a"
    graphMessageText="Loading graph"
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
  timePickerIncrement: 30,

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
    function() {
      const postObj = {
        startTime: moment(this.get('viewAnomalyStart')).utc().valueOf(),
        endTime: moment(this.get('viewAnomalyEnd')).utc().valueOf(),
        feedbackType: this.get('isNewTrend') ? 'ANOMALY_NEW_TREND' : 'ANOMALY',
        dimension: this.get('selectedDimension') || null,
        comment: this.get('anomalyComments') || null,
        externalURL: this.get('anomalyLinks') || null
      };

      return postObj;
    }
  ),

  /**
   * Sends post object as is to parent
   * @method bubbleModalInput
   */
  bubbleModalInput() {
    this.sendAction('inputAction', this.get('reportAnomalyPayload'));
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
     * Handle selected dimension filter
     * @method onSelectDimension
     * @param {Object} selectedObj - the user-selected dimension to filter by
     */
    onFeedbackTypeSelection(trendSelection) {
      this.set('isNewTrend', trendSelection);
      this.bubbleModalInput();
    },

    /**
     * Handle selected dimension filter
     * @method onFeedbackComments
     * @param {String} comment field value
     */
    onAnomalyInput(value) {
      this.bubbleModalInput();
    }

  }
});

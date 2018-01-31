import Component from '@ember/component';
import { computed, setProperties, getProperties } from '@ember/object';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import moment from 'moment';
import fetch from 'fetch';

export default Component.extend({
  //
  // external properties
  //
  selectedUrn: null,

  anomalyRange: null,

  entities: null,

  //
  // internal properties
  //

  /**
   * Max available data range for the selected metric.
   * @type {Int}
   */
  maxTime: null,

  didReceiveAttrs() {
    const { selectedUrn, anomalyRange } = getProperties(this, 'selectedUrn', 'anomalyRange');

    if (!selectedUrn || !anomalyRange) { return; }

    if (!selectedUrn.startsWith('thirdeye:metric:')) { return; }

    const id = selectedUrn.split(':')[2];

    fetch(`/data/maxDataTime/metricId/${id}`)
      .then(checkStatus)
      .then(res => setProperties(this, { maxTime: res }));
  },

  /**
   * True, if the analysis period exceeds the max available data range
   * @type {boolean}
   */
  hasMaxTimeWarning: computed('anomalyRange', 'maxTime', function () {
    const { anomalyRange, maxTime } = getProperties(this, 'anomalyRange', 'maxTime');

    if (!anomalyRange || !maxTime) { return false; }

    return anomalyRange[1] > maxTime;
  }),

  /**
   * Formatted max available data range
   * @type {string}
   */
  maxTimeFormatted: computed('maxTime', function () {
    const { maxTime } = getProperties(this, 'maxTime');
    return moment(maxTime).format('MMM D, hh:mm a');
  })

});

import { isPresent } from '@ember/utils';
import moment from 'moment';
import _ from 'lodash';
import {
  checkStatus,
  postProps
} from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';
import { getAnomalyDataUrl } from 'thirdeye-frontend/utils/api/anomaly';

/**
 * Response type options for anomalies
 */
export const anomalyResponseObj = [
  { name: 'Not reviewed yet',
    value: 'NO_FEEDBACK',
    status: 'Not Resolved'
  },
  { name: 'True anomaly',
    value: 'ANOMALY',
    status: 'Confirmed Anomaly'
  },
  { name: 'False alarm',
    value: 'NOT_ANOMALY',
    status: 'False Alarm'
  },
  { name: 'Confirmed - New Trend',
    value: 'ANOMALY_NEW_TREND',
    status: 'New Trend'
  }
];

/**
 * Update feedback status on any anomaly
 * @method updateAnomalyFeedback
 * @param {Number} anomalyId - the id of the anomaly to update
 * @param {String} feedbackType - key for feedback type
 * @return {Ember.RSVP.Promise}
 */
export function updateAnomalyFeedback(anomalyId, feedbackType) {
  const url = `/anomalies/updateFeedback/${anomalyId}`;
  const data = { feedbackType, comment: '' };
  return fetch(url, postProps(data)).then((res) => checkStatus(res, 'post'));
}

/**
 * Fetch a single anomaly record for verification
 * @method verifyAnomalyFeedback
 * @param {Number} anomalyId
 * @return {undefined}
 */
export function verifyAnomalyFeedback(anomalyId) {
  const anomalyUrl = getAnomalyDataUrl() + anomalyId;
  return fetch(anomalyUrl).then(checkStatus);
}

/**
 * Formats anomaly duration property for display on the table
 * @param {Number} anomalyStart - the anomaly start time
 * @param {Number} anomalyEnd - the anomaly end time
 * @returns {String} the fomatted duration time
 * @example getFormatDuration(1491804013000, 1491890413000) // yields => 'Apr 9, 11:00 PM'
 */
export function getFormatedDuration(anomalyStart, anomalyEnd) {
  const startMoment = moment(anomalyStart);
  const endMoment = moment(anomalyEnd);
  const anomalyDuration = moment.duration(endMoment.diff(startMoment));
  const days = anomalyDuration.get('days');
  const hours = anomalyDuration.get('hours');
  const minutes = anomalyDuration.get('minutes');
  const duration = [pluralizeTime(days, 'day'), pluralizeTime(hours, 'hour'), pluralizeTime(minutes, 'minute')];
  // We want to display only non-zero duration values in our table
  const noZeroDurationArr = _.remove(duration, function(item) {
    return isPresent(item);
  });
  return noZeroDurationArr.join(', ');
}

/**
 * Pluralizes and formats the anomaly range duration string
 * @param {Number} time
 * @param {String} unit
 * @returns {String}
 * @example pluralizeTime(days, 'day')
 */
export function pluralizeTime(time, unit) {
  const unitStr = time > 1 ? unit + 's' : unit;
  return time ? time + ' ' + unitStr : '';
}

export default {
  anomalyResponseObj,
  getAnomalyDataUrl,
  updateAnomalyFeedback,
  getFormatedDuration,
  verifyAnomalyFeedback,
  pluralizeTime
};

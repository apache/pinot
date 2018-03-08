import moment from 'moment';
import { isPresent } from '@ember/utils';
import _ from 'lodash';

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
  getFormatedDuration,
  pluralizeTime
};

/*
 * Util functions to structure the data to be passed into individual columns
 */

import moment from 'moment';
import * as anomalyUtil from 'thirdeye-frontend/utils/anomaly';

/*
 * Convert anomaly 'start' and 'end' into an object
 *
 * @param {Number} start
 *   The start time in milliseconds.
 * @param {Number} end
 *   The end time in milliseconds
 * @param {Boolean} showLink
 *   Indicates whether to hyperlink the start duration
 *
 * @returns {Object}
 *   Description of the object.
 */
export const getAnomaliesStartDuration = (start, end, showLink = false) => {
  return {
    startTime: start,
    endTime: end,
    duration: moment.duration(end - start).asHours() + ' hours',
    showLink
  };
};

/*
 * Return feedbackObject with pre-selected 'feedback' to be use in the feedback dropdown
 *
 * @param {String} feedback
 *   The object containing the list of anomalies and their count.
 *
 * @returns {Object} feedbackObject
 *   Description of the object.
 */
export const getFeedback = (feedback) => {
  const selectedFeedback = feedback
    ? anomalyUtil.anomalyResponseObj.find((f) => f.value === feedback.feedbackType)
    : anomalyUtil.anomalyResponseObj[0];

  return {
    options: anomalyUtil.anomalyResponseObj.mapBy('name'),
    selected: selectedFeedback ? selectedFeedback.name : anomalyUtil.anomalyResponseObj[0].name
  };
};

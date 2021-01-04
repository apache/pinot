import { isPresent } from '@ember/utils';
import moment from 'moment';
import _ from 'lodash';
import { checkStatus, postProps, getProps, putProps } from 'thirdeye-frontend/utils/utils';
import { postYamlProps } from 'thirdeye-frontend/utils/yaml-tools';
import fetch from 'fetch';
import {
  anomalyApiUrls,
  getAnomaliesForYamlPreviewUrl,
  getAnomaliesByAlertIdUrl,
  getPerformanceStatsByAlertIdUrl,
  getAnomalyFiltersByTimeRangeUrl,
  getAnomalyFiltersByAnomalyIdUrl,
  getBoundsUrl,
  getAiAvailabilityUrl
} from 'thirdeye-frontend/utils/api/anomaly';
import { yamlAPI } from 'thirdeye-frontend/utils/api/self-serve';

/**
 * Response type options for anomalies.
 * TODO: This needs to be a configuration somewhere for the user to save. It is hard coded here but this is not a good idea
 to hard code it for feedback mapping.
 */
export const anomalyResponseObj = [
  { name: 'Not reviewed yet', value: 'NO_FEEDBACK', status: 'Not Resolved' },
  { name: 'Yes - unexpected', value: 'ANOMALY', status: 'Confirmed Anomaly' },
  { name: 'Expected temporary change', value: 'ANOMALY_EXPECTED', status: 'Expected Anomaly' },
  { name: 'Expected permanent change', value: 'ANOMALY_NEW_TREND', status: 'Confirmed - New Trend' },
  { name: 'No change observed', value: 'NOT_ANOMALY', status: 'False Alarm' }
];

export const reportedAnomalyResponseObj = [
  { name: 'Not reviewed yet', value: 'NONE', status: 'Not Resolved' },
  { name: 'Yes - unexpected', value: 'FALSE_NEGATIVE', status: 'Confirmed Anomaly' }
];

export const anomalySeverityLevelObj = [
  { name: 'Default', value: 'DEFAULT' },
  { name: 'Low', value: 'LOW' },
  { name: 'Medium', value: 'MEDIUM' },
  { name: 'High', value: 'HIGH' },
  { name: 'Critical', value: 'CRITICAL' }
];

export const anomalyTypeMapping = {
  DEVIATION: 'Metric Deviation',
  TREND_CHANGE: 'Trend Change',
  DATA_SLA: 'SLA Violation'
};

/**
 * Mapping for anomalyResponseObj 'value' to 'name' for easy lookup
 */
export const anomalyResponseMap = {};
anomalyResponseObj.forEach((obj) => {
  anomalyResponseMap[obj.value] = obj.name;
});

/**
 * Mapping for anomalyResponseObjAll 'value' to 'name' for easy lookup
 */
export const anomalyResponseMapAll = {};
[...anomalyResponseObj, ...reportedAnomalyResponseObj].forEach((obj) => {
  anomalyResponseMapAll[obj.value] = obj.name;
});

/**
 * Update feedback status on any anomaly
 * @method updateAnomalyFeedback
 * @param {Number} anomalyId - the id of the anomaly to update
 * @param {String} feedbackType - key for feedback type
 * @return {Ember.RSVP.Promise}
 */
export function updateAnomalyFeedback(anomalyId, feedbackType) {
  const url = `/dashboard/anomaly-merged-result/feedback/${anomalyId}`;
  const data = { feedbackType, comment: '' };
  return fetch(url, postProps(data)).then((res) => checkStatus(res, 'post'));
}

/**
 * Post Yaml to preview anomalies detected with given configuration
 * @method getYamlPreviewAnomalies
 * @param {String} yamlString - the alert configuration in Yaml
 * @param {Number} startTime - start time of analysis range
 * @param {Number} endTime - end time of analysis range
 * @return {Ember.RSVP.Promise}
 */
export function getYamlPreviewAnomalies(yamlString, startTime, endTime, alertId) {
  const url = getAnomaliesForYamlPreviewUrl(startTime, endTime, alertId);
  return fetch(url, postYamlProps(yamlString)).then((res) => checkStatus(res, 'post', false, true));
}

/**
 * Get bounds for a given detection (note the anomalies in this response are not end-user anomalies)
 * @method getBoundsAndAnomalies
 * @param {String} detectionId - the id of the detection
 * @param {Number} startTime - start time of analysis range
 * @param {Number} endTime - end time of analysis range
 * @return {Ember.RSVP.Promise}
 */
export function getBounds(detectionId, startTime, endTime) {
  const url = getBoundsUrl(detectionId, startTime, endTime);
  return fetch(url, getProps()).then((res) => checkStatus(res));
}

/**
 * Get table data for AI Availability
 * @method getAiAvailability
 * @param {Number} detectionConfigId - the config id for the table data's alert
 * @param {Number} startDate - start time of analysis range
 * @param {Number} endDate - end time of analysis range
 * @return {Ember.RSVP.Promise}
 */
export function getAiAvailability(detectionConfigId, startDate, endDate) {
  const url = getAiAvailabilityUrl(startDate, endDate);
  return fetch(url).then(checkStatus);
}

/**
 * Get anomalies for a given detection id over a specified time range
 * @method getAnomaliesByAlertId
 * @param {Number} alertId - the alert id aka detection config id
 * @param {Number} startTime - start time of analysis range
 * @param {Number} endTime - end time of analysis range
 * @return {Ember.RSVP.Promise}
 */
export function getAnomaliesByAlertId(alertId, startTime, endTime) {
  const url = getAnomaliesByAlertIdUrl(alertId, startTime, endTime);
  return fetch(url).then(checkStatus);
}

/**
 * Get anomaly filters over a specified time range
 * @method getAnomalyFiltersByTimeRange
 * @param {Number} startTime - start time of query range
 * @param {Number} endTime - end time of query range
 * @return {Ember.RSVP.Promise}
 */
export function getAnomalyFiltersByTimeRange(startTime, endTime) {
  const url = getAnomalyFiltersByTimeRangeUrl(startTime, endTime);
  return fetch(url).then(checkStatus);
}

/**
 * Get anomaly filters for specified anomaly ids
 * @method getAnomalyFiltersByAnomalyId
 * @param {Number} startTime - start time of query range
 * @param {Number} endTime - end time of query range
 * @param {String} anomalyIds - string of comma delimited anomaly ids
 * @return {Ember.RSVP.Promise}
 */
export function getAnomalyFiltersByAnomalyId(startTime, endTime, anomalyIds) {
  const url = getAnomalyFiltersByAnomalyIdUrl(startTime, endTime, anomalyIds);
  return fetch(url).then(checkStatus);
}

/**
 * Get performance stats for a given detection id over a specified time range
 * @method getPerformanceStatsByAlertId
 * @param {Number} alertId - the alert id aka detection config id
 * @param {Number} startTime - start time of analysis range
 * @param {Number} endTime - end time of analysis range
 * @return {Ember.RSVP.Promise}
 */
export function getPerformanceStatsByAlertId(alertId, startTime, endTime) {
  const url = getPerformanceStatsByAlertIdUrl(alertId, startTime, endTime);
  return fetch(url).then(checkStatus);
}

/**
 * Put alert active status change into backend
 * @method putAlertActiveStatus
 * @param {Number} detectionConfigId - id of alert's detection configuration
 * @param {Boolean} active - what to set active flag to
 * @return {Ember.RSVP.Promise}
 */
export function putAlertActiveStatus(detectionConfigId, active) {
  const url = yamlAPI.setAlertActivationUrl(detectionConfigId, active);
  return fetch(url, putProps()).then(checkStatus);
}

/**
 * Fetch a single anomaly record for verification
 * @method verifyAnomalyFeedback
 * @param {Number} anomalyId
 * @return {Ember.RSVP.Promise}
 */
export function verifyAnomalyFeedback(anomalyId) {
  const url = `${anomalyApiUrls.getAnomalyDataUrl(anomalyId)}`;
  return fetch(url).then(checkStatus);
}

/**
 * Formats anomaly duration property for display on the table
 * @param {Number} anomalyStart - the anomaly start time
 * @param {Number} anomalyEnd - the anomaly end time
 * @returns {String} the fomatted duration time
 * @example getFormatDuration(1491804013000, 1491890413000) // yields => 'Apr 9, 11:00 PM'
 */
export function getFormattedDuration(anomalyStart, anomalyEnd) {
  const startMoment = moment(anomalyStart);
  const endMoment = moment(anomalyEnd);
  const anomalyDuration = moment.duration(endMoment.diff(startMoment));
  const days = anomalyDuration.get('days');
  const hours = anomalyDuration.get('hours');
  const minutes = anomalyDuration.get('minutes');
  const duration = [pluralizeTime(days, 'day'), pluralizeTime(hours, 'hour'), pluralizeTime(minutes, 'minute')];
  // We want to display only non-zero duration values in our table
  const noZeroDurationArr = _.remove(duration, function (item) {
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

export function searchAnomaly(offset, limit, startTime, endTime, anomalyIds) {
  return searchAnomalyWithFilters(offset, limit, startTime, endTime, [], [], [], [], [], anomalyIds);
}

export function searchAnomalyWithFilters(
  offset,
  limit,
  startTime,
  endTime,
  feedbackStatuses,
  subscriptionGroups,
  detectionNames,
  metrics,
  datasets,
  anomalyIds
) {
  let url = `/anomaly-search?offset=${offset}&limit=${limit}`;
  if (startTime) {
    url = url.concat(`&startTime=${startTime}`);
  }
  if (endTime) {
    url = url.concat(`&endTime=${endTime}`);
  }
  feedbackStatuses = feedbackStatuses || [];
  for (const feedbackStatus of feedbackStatuses) {
    const feedback = anomalyResponseObj.find((feedback) => feedback.name === feedbackStatus);
    if (feedback) {
      url = url.concat(`&feedbackStatus=${feedback.value}`);
    }
  }
  subscriptionGroups = subscriptionGroups || [];
  for (const subscriptionGroup of subscriptionGroups) {
    url = url.concat(`&subscriptionGroup=${subscriptionGroup}`);
  }
  detectionNames = detectionNames || [];
  for (const detectionName of detectionNames) {
    url = url.concat(`&detectionName=${detectionName}`);
  }
  metrics = metrics || [];
  for (const metric of metrics) {
    url = url.concat(`&metric=${metric}`);
  }
  datasets = datasets || [];
  for (const dataset of datasets) {
    url = url.concat(`&dataset=${dataset}`);
  }
  anomalyIds = anomalyIds || [];
  for (const anomalyId of anomalyIds) {
    url = url.concat(`&anomalyId=${anomalyId}`);
  }
  return fetch(url).then(checkStatus);
}

export default {
  anomalyResponseObj,
  anomalyResponseMap,
  anomalyResponseMapAll,
  updateAnomalyFeedback,
  getFormattedDuration,
  verifyAnomalyFeedback,
  pluralizeTime,
  putAlertActiveStatus,
  getYamlPreviewAnomalies,
  getAnomaliesByAlertId,
  getBounds,
  searchAnomaly,
  searchAnomalyWithFilters,
  anomalyTypeMapping,
  anomalySeverityLevelObj
};

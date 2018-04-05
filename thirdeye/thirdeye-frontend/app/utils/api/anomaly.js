/**
 * Returns the anomaly data url
 * @param {Number} startStamp - the anomaly start time
 * @param {Number} endStamp - the anomaly end time
 * @returns {String} the complete anomaly data url
 * @example getAnomalyDataUrl(1491804013000, 1491890413000) // yields => /anomalies/search/anomalyIds/1491804013000/1491890413000/1?anomalyIds=
 */
export function getAnomalyDataUrl(startStamp = 0, endStamp = 0) {
  return `/anomalies/search/anomalyIds/${startStamp}/${endStamp}/1?anomalyIds=`;
}

/**
 * Returns the application performance details
  * @param {String} appName - the application name
  * @param {Number} startStamp - the anomaly iso start time
  * @param {Number} endStamp - the anomaly iso end time
 * @returns {String} the complete Anomalies By AppName url
 * @example getPerformanceByAppNameUrl('someAppName', 1508472800000) // yields => /detection-job/eval/application/lms-ads?start=2017-09-01T00:00:00Z&end=2018-04-01T00:00:00Z
 */
export function getPerformanceByAppNameUrl(appName, startTime, endTime) {
  return `/detection-job/eval/application/${appName}?start=${startTime}&end=${endTime}`;
}

/**
 * Returns the Anomalies By AppName url
  * @param {String} appName - the application name
 * @param {Number} startStamp - the anomaly start time
 * @returns {String} the complete Anomalies By AppName url
 * @example getAnomaliesByAppNameUrl('someAppName', 1508472800000) // yields => /userdashboard/anomalies?application=someAppName&start=1508472800000
 */
export function getAnomaliesByAppNameUrl(appName, startTime) {
  return `/userdashboard/anomalies?application=${appName}&start=${startTime}`;
}

export const anomalyApiUrls = {
  getAnomalyDataUrl,
  getAnomaliesByAppNameUrl,
  getPerformanceByAppNameUrl
};

export default {
  anomalyApiUrls
};

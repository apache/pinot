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

export default {
  getAnomalyDataUrl
};

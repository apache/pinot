/**
 * Converts a float to percent
 * @example floatToPercent(0.553) --> "55.3%"
 * @param {float} float
 * @return {String} - percent value of float with a % sign
 */
export default function floatToPercent(float) {
  return Number.isNaN(float) ? 'NaN' : `${(float * 100).toFixed(2)}%`;
}

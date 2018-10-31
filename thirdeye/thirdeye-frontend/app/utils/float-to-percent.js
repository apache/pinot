/**
 * Converts a float to percent
 * @example floatToPercent(0.55333) --> 55.3
 * @param {float} float
 * @return {float} - percentage value of float
 */
export default function floatToPercent(float) {
  return Number.isNaN(Number(float)) ? '-' : (float * 100).toFixed(2);
}

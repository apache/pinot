import { helper } from '@ember/component/helper';
import { toColorDirection } from 'thirdeye-frontend/utils/rca-utils';

/**
 * Template helper that computes the color
 * for delta changes
 * @param {Number} [value='N/A'] Contribution change in percentage
 * @param {Boolean} [inverse=false] invert coloring
 * @return {String} positive, neutral, or negative modifier for BEM class
 */
export function colorDelta([value = 'N/A', inverse = false]) {
  if (value ===  'N/A') { return 'neutral'; }
  return toColorDirection(value, inverse);
}

export default helper(colorDelta);

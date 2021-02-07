import { helper } from '@ember/component/helper';

/**
 * Template helper that computes the text color
 * for the contribution map
 * @param {Number} [value=0] Contribution change in percentage
 * @return {String}          Text color (HEX)
 */
export function computeTextColor([value = 0]) {
  const opacity = Math.abs(value / 25);

  return opacity < 0.5 ? '#000000' : '#ffffff';
}

export default helper(computeTextColor);

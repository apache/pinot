import { helper } from '@ember/component/helper';

/**
 * Template helper that computes the background color
 * for the contribution map
 * @param {Number} [value=0] Contribution change in percentage
 * @return {String}          rgba color for background
 */
export function computeColor([value = 0]) {
  const opacity = Math.abs(value / 25);

  if (value > 0) {
    return `rgba(0,0,234,${opacity})`;
  } else{
    return `rgba(234,0,0,${opacity})`;
  }
}

export default helper(computeColor);

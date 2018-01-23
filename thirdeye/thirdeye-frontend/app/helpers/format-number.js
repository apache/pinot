import { helper } from '@ember/component/helper';
import d3 from 'd3';

/**
 * Displays human readable number
 * @param {number} num A number
 * @return {string} human readable number
 */
export function formatNumber([num=0], {formatter, format}) {
  if (formatter && format) {
    return d3.format(format)(num);
  }

  return num.toString().replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
}

export default helper(formatNumber);

import { helper } from '@ember/component/helper';
import d3 from 'd3';

/**
 * Displays human readable number
 * @param {number} num A number
 * @return {string} human readable number
 * @example <caption>Example usage (default).</caption>
 * // returns 123,456,789
 * {{formatNumber 123456789}}
 * @example <caption>Example usage (d3 formatter).</caption>
 * // returns 123M
 * {{formatNumber 123456789 formatter="d3" format=".3s"}}
 */
export function formatNumber([num = 0], { formatter = null, format = null } = { formatter, format }) {
  if (formatter && format) {
    return d3.format(format)(num);
  }

  return num.toString().replace(/(\d)(?=(\d\d\d)+(?!\d))/g, '$1,');
}

export default helper(formatNumber);

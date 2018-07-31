import { helper } from '@ember/component/helper';

/**
 * Returns true if the set does not contain the value
 * @param {Object} set - the set
 * @param {String} value - the value to check for
 * @return {Boolean} !set.has(value)
 */
export function isByTwo(num) {
  return num % 2 === 0;
}

export default helper(isByTwo);

import { helper } from '@ember/component/helper';

/**
 * Returns true if the set does not contain the value
 * @param {Object} set - the set
 * @param {String} value - the value to check for
 * @return {Boolean} !set.has(value)
 */
export function setHasNot([set = new Set(), value = null]) {
  return !set.has(value);
}

export default helper(setHasNot);

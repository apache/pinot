import { helper } from '@ember/component/helper';

/**
 * Returns the value referred to by key in dict. Avoids resolving dot-syntax as done by the builtin
 * Ember {{get}} helper to gracefully handle keys with "." (dot).
 *
 * @param {Object} dict - the dictionary/object
 * @param {String} key - the key
 * @return {Object} dict[key]
 */
export function getSafe([dict, key]) {
  if (!dict) { return; }
  return dict[key];
}

export default helper(getSafe);


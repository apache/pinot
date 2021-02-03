/**
 * Translates a value to make it human readable based on a given dictionary
 * Note: only translates from value to key, NOT key to value
 * @param {Object} mapping - a dictionary that maps a non-human readable word to its intended translation
 * @param {String} value - value to translate
 * @return {String}
 * @example
 * mapping = {
 *  '1 hour': '1_HOURS',
 *  '1 day': '1_DAYS'
 * }
 * value = '1_HOURS'
 * translate(mapping, value) ---> '1 hour'
 */
export default (mapping, value) => {
  return Object.keys(mapping).find((key) => value === mapping[key]);
};

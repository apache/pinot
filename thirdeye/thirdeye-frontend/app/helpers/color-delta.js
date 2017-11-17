import Ember from 'ember';

/**
 * Template helper that computes the color
 * for delta changes
 * @param {Number} [value='N/A'] Contribution change in percentage
 * @return {String}          positive or negative modifier for BEM class
 */
export function colorDelta([value = 'N/A']) {
  if (value ===  'N/A') { return; }
  return parseInt(value * 100) >= 0 ? 'positive' : 'negative';
}

export default Ember.Helper.helper(colorDelta);

import Ember from 'ember';

/**
 * Template helper that computes the color
 * for delta changes
 * @param {Number} [value='N/A'] Contribution change in percentage
 * @return {String}          positive or negative modifier for BEM class
 */
export function colorDelta([value = 'N/A']) {
  let sign = true;
  if (value ===  'N/A') { return; }
  
  if (Number.isFinite(value)) {
    sign = parseInt(value * 100) >= 0;
  } else {
    sign = Math.sign(value);
  }
  return sign ? 'positive' : 'negative';
}

export default Ember.Helper.helper(colorDelta);

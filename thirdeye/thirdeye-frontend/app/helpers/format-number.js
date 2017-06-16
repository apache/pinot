import Ember from 'ember';

/**
 * Displays human readable number
 * @param {number} num A number
 * @return {string} human readable number
 */
export function formatNumber([num=0]/*, hash*/) {
  return num.toString().replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
}

export default Ember.Helper.helper(formatNumber);

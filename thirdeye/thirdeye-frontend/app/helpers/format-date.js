import Ember from 'ember';
import moment from 'moment';

/**
 * Template Helper that displays human readable date
 * @param {string} date A date
 * @param {string} granularity granularity of the metric
 * @return {string} human readable date
 */
export function formatDate([date, granularity='DAYS']) {
  const dateFormat = {
    'DAYS': 'M/D',
    'HOURS': 'M/D ha',
    '5_MINUTES': 'M/D hh:mm a'
  }[granularity];
  const momentDate = moment(date);

  return momentDate.isValid() && dateFormat
    ? momentDate.format(dateFormat)
    : '';
}

export default Ember.Helper.helper(formatDate);
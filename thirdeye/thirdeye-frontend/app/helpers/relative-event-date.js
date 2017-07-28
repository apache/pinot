import Ember from 'ember';
import moment from 'moment';

/**
 * Template helper that turns a relative event date into a human
 * readable format
 * @param {Number} [value=0] offset in millis
 * @return {String}          human readable string
 */
export function relativeEventDate([value = 0]) {
  if (value >= 0) {
    const out = moment.duration(value).humanize();
    return `${out} later`;
  } else{
    const out = moment.duration(-1 * value).humanize();
    return `${out} earlier`;
  }
}

export default Ember.Helper.helper(relativeEventDate);

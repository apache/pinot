import Ember from 'ember';
import moment from 'moment';

/**
 * Template helper that turns an event duration into a human
 * readable format
 * @param {Number} [start=0] start in epoch millis
 * @param {Number} [end=0]   end in epoch millis
 * @param {Number} [thres=0] threshold to skip in millis
 * @return {String}          human readable string
 */
export function eventDuration([start = 0, end = 0, thres = 0]) {
  const mom = moment.duration(end - start);
  if (mom.asMilliseconds() < thres)
    return '< ' + moment.duration(thres).humanize();
  return mom.humanize();
}

export default Ember.Helper.helper(eventDuration);

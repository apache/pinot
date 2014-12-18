package com.linkedin.pinot.core.time;

import com.linkedin.pinot.common.utils.time.TimeConstants;


/**
 * Jul 11, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 * expects everything to be in UTC
 */

public enum SegmentTimeUnit {
  millis,
  seconds,
  minutes,
  hours,
  days,
  weeks,
  months,
  years;

  public static long toMillis(SegmentTimeUnit timeUnit) {
    if (timeUnit == null) {
      timeUnit = days;
    }
    if (timeUnit == days) {
      return TimeConstants.ONE_DAY_IN_MILLIS;
    } else if (timeUnit == hours) {
      return TimeConstants.ONE_HOUR_IN_MILLIS;
    } else if (timeUnit == weeks) {
      return TimeConstants.ONE_WEEK_IN_MILLIS;
    } else if (timeUnit == minutes) {
      return TimeConstants.ONE_MINUTE_IN_MILLIS;
    } else if (timeUnit == seconds) {
      return TimeConstants.ONE_SECOND_IN_MILLIS;
    } else if (timeUnit == months) {
      return TimeConstants.ONE_MONTH_IN_MILLIS;
    } else if (timeUnit == years) {
      return TimeConstants.ONE_YEAR_IN_MILLIS;
    } else if (timeUnit == millis) {
      return 1;
    } else {
      throw new UnsupportedOperationException("Input string: " + timeUnit
          + ", Accepted strings: 'seconds', 'minutes', 'hours', 'days', 'weeks', "
          + "'months', 'years'");
    }
  }

  public static long toMillis(SegmentTimeUnit segmentTimeUnit, String timeValue) {
    long timeValueInLong = Long.parseLong(timeValue);
    if (segmentTimeUnit == days) {
      timeValueInLong *= TimeConstants.ONE_DAY_IN_MILLIS;
    } else if (segmentTimeUnit == weeks) {
      timeValueInLong *= TimeConstants.ONE_WEEK_IN_MILLIS;
    } else if (segmentTimeUnit == months) {
      timeValueInLong *= TimeConstants.ONE_MONTH_IN_MILLIS;
    } else if (segmentTimeUnit == years) {
      timeValueInLong *= TimeConstants.ONE_YEAR_IN_MILLIS;
    } else if (segmentTimeUnit == hours) {
      timeValueInLong *= TimeConstants.ONE_HOUR_IN_MILLIS;
    } else if (segmentTimeUnit == minutes) {
      timeValueInLong *= TimeConstants.ONE_MINUTE_IN_MILLIS;
    } else if (segmentTimeUnit == seconds) {
      timeValueInLong *= TimeConstants.ONE_SECOND_IN_MILLIS;
    }
    return timeValueInLong;
  }
}

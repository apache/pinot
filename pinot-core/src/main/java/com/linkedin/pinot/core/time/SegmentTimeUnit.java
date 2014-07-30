package com.linkedin.pinot.core.time;

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
}

package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;


/**
 * TimeRangeEntity represents a time-range as investigated by the user for purposes of
 * root cause search. The URN namespace is defined as 'thirdeye:timerange:{start}:{end}'.
 */
public class TimeRangeEntity extends Entity {
  public static TimeRangeEntity fromURN(String urn, double score) {
    EntityUtils.assertType(urn, EntityType.TIMERANGE);
    String[] parts = urn.split(":");
    return new TimeRangeEntity(urn, 1.0, Long.parseLong(parts[2]), Long.parseLong(parts[3]));
  }

  public static TimeRangeEntity fromRange(long start, long end) {
    String urn = EntityType.TIMERANGE.formatUrn("%d:%d", start, end);
    return new TimeRangeEntity(urn, 1.0, start, end);
  }

  final long start;
  final long end;

  public TimeRangeEntity(String urn, double score, long start, long end) {
    super(urn, score);
    this.start = start;
    this.end = end;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }
}

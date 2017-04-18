package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;


/**
 * BaselineEntity represents a time-range used for comparison to a {@code TimeRange} entity.
 * The URN namespace is defined as 'thirdeye:baseline:{start}:{end}'.
 */
public class BaselineEntity extends Entity {
  public static BaselineEntity fromURN(String urn, double score) {
    EntityUtils.assertType(urn, EntityUtils.EntityType.BASELINE);
    String[] parts = urn.split(":");
    return new BaselineEntity(urn, score, Long.parseLong(parts[2]), Long.parseLong(parts[3]));
  }

  public static BaselineEntity fromRange(double score, long start, long end) {
    String urn = EntityUtils.EntityType.BASELINE.formatUrn("%d:%d", start, end);
    return new BaselineEntity(urn, score, start, end);
  }

  final long start;
  final long end;

  public BaselineEntity(String urn, double score, long start, long end) {
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

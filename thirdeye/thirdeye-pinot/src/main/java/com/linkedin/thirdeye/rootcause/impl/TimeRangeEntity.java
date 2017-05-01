package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import java.util.HashSet;
import java.util.Set;


/**
 * TimeRangeEntity represents a time-range as investigated by the user for purposes of
 * root cause search. The URN namespace is defined as 'thirdeye:timerange:{start}:{end}'.
 */
public class TimeRangeEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:timerange:");

  public static final String TYPE_CURRENT = "current";
  public static final String TYPE_BASELINE = "baseline";

  private final String type;
  private final long start;
  private final long end;

  protected TimeRangeEntity(String urn, double score, String type, long start, long end) {
    super(urn, score);
    this.type = type;
    this.start = start;
    this.end = end;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public String getType() {
    return type;
  }

  public static TimeRangeEntity fromURN(String urn, double score) {
    if(!TYPE.isType(urn))
      throw new IllegalArgumentException(String.format("URN '%s' is not type '%s'", urn, TYPE.getPrefix()));
    String[] parts = urn.split(":", 5);
    if(parts.length != 5)
      throw new IllegalArgumentException(String.format("Timerange URN must have 5 parts but has '%d'", parts.length));
    return fromRange(score, parts[2], Long.valueOf(parts[3]), Long.valueOf(parts[4]));
  }

  public static TimeRangeEntity fromRange(double score, String type, long start, long end) {
    String urn = TYPE.formatURN(type, start, end);
    return new TimeRangeEntity(urn, score, type, start, end);
  }

  /**
   * Returns the TimeRangeEntity contained in the search context of an execution context.
   * Expects exactly one TimeRange entity and returns {@code null} if none or multiple
   * time range entities are found. If the search context contains an instance of
   * TimeRangeEntity it returns the instance. Otherwise, constructs a new instance of
   * TimeRangeEntity from an encoding URN.
   *
   * @param context execution context
   * @return TimeRangeEntity
   */
  public static TimeRangeEntity getContextTimeRange(PipelineContext context, String type) {
    Set<TimeRangeEntity> timeRanges = context.filter(TimeRangeEntity.class);
    Set<TimeRangeEntity> matching = new HashSet<>();
    for(TimeRangeEntity e : timeRanges) {
      if(e.getType().equals(type))
        matching.add(e);
    }
    if(matching.size() != 1)
      throw new IllegalArgumentException(String.format("Must contain exactly one of type '%s'", type));
    return matching.iterator().next();
  }

  public static TimeRangeEntity getContextCurrent(PipelineContext context) {
    return getContextTimeRange(context, TYPE_CURRENT);
  }

  public static TimeRangeEntity getContextBaseline(PipelineContext context) {
    return getContextTimeRange(context, TYPE_BASELINE);
  }
}

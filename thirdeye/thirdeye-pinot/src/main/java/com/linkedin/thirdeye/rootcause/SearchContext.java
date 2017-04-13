package com.linkedin.thirdeye.rootcause;

import java.util.Collections;
import java.util.Set;


public class SearchContext {
  final Set<Entity> entities;
  final long timestampStart;
  final long timestampEnd;
  final long baselineStart;
  final long baselineEnd;

  public SearchContext(Set<Entity> entities, long timestampStart, long timestampEnd, long baselineStart, long baselineEnd) {
    this.entities = entities;
    this.timestampStart = timestampStart;
    this.timestampEnd = timestampEnd;
    this.baselineStart = baselineStart;
    this.baselineEnd = baselineEnd;
  }

  public Set<Entity> getEntities() {
    return Collections.unmodifiableSet(entities);
  }

  public long getTimestampStart() {
    return timestampStart;
  }

  public long getTimestampEnd() {
    return timestampEnd;
  }

  public long getBaselineStart() {
    return baselineStart;
  }

  public long getBaselineEnd() {
    return baselineEnd;
  }
}

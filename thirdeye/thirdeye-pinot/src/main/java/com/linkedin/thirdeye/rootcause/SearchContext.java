package com.linkedin.thirdeye.rootcause;

import java.util.Set;


public class SearchContext {
  final Set<Entity> entities;
  final long timestampStart;
  final long timestampEnd;

  public SearchContext(Set<Entity> entities, long timestampStart, long timestampEnd) {
    this.entities = entities;
    this.timestampStart = timestampStart;
    this.timestampEnd = timestampEnd;
  }

  public Set<Entity> getEntities() {
    return entities;
  }

  public long getTimestampStart() {
    return timestampStart;
  }

  public long getTimestampEnd() {
    return timestampEnd;
  }
}

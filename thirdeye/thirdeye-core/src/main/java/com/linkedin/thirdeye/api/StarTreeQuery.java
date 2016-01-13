package com.linkedin.thirdeye.api;

import java.util.Map;
import java.util.Set;

/**
 * A getAggregate on a {@link StarTree} that returns metrics for a dimension combination.
 */
public interface StarTreeQuery {
  /** @return A set of dimension names whose values are "*" for this getAggregate */
  Set<String> getStarDimensionNames();

  DimensionKey getDimensionKey();

  TimeRange getTimeRange();
}

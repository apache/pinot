package com.linkedin.thirdeye.api;

import java.util.Map;
import java.util.Set;

/**
 * A getAggregate on a {@link StarTree} that returns metrics for a dimension combination.
 *
 * <p>
 *   If {@link #getTimeBuckets()} != null, then {@link #getTimeRange()} == null, and vice-versa.
 * </p>
 */
public interface StarTreeQuery
{
  /** @return A set of dimension names whose values are "*" for this getAggregate */
  Set<String> getStarDimensionNames();

  /** @return The dimension values (either specific or "*") to getAggregate */
  Map<String, String> getDimensionValues();

  /**
   * @return
   *  The discrete time buckets for an IN style getAggregate, or null if not applicable
   */
  Set<Long> getTimeBuckets();

  /**
   * @return
   *  The start and end time for a BETWEEN style getAggregate, or null if not applicable
   */
  Map.Entry<Long, Long> getTimeRange();
}

package com.linkedin.thirdeye.api;

import java.util.Map;
import java.util.Set;

/**
 * A query on a {@link StarTree} that returns metrics for a dimension combination.
 *
 * <p>
 *   If {@link #getTimeBuckets()} != null, then {@link #getTimeRange()} == null, and vice-versa.
 * </p>
 */
public interface StarTreeQuery
{
  /** @return A set of dimension names whose values are "*" for this query */
  Set<String> getStarDimensionNames();

  /** @return The dimension values (either specific or "*") to query */
  Map<String, String> getDimensionValues();

  /**
   * @return
   *  The discrete time buckets for an IN style query, or null if not applicable
   */
  Set<Long> getTimeBuckets();

  /**
   * @return
   *  The start and end time for a BETWEEN style query, or null if not applicable
   */
  Map.Entry<Long, Long> getTimeRange();
}

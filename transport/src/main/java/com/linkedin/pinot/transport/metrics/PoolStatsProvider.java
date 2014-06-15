package com.linkedin.pinot.transport.metrics;

import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;

public interface PoolStatsProvider<T extends Sampling & Summarizable> {

  /**
   * Get a snapshot of pool statistics. The specific statistics are described in
   * {@link PoolStats}. Calling getStats will reset any 'latched' statistics.
   *
   * @return An {@link PoolStats} object representing the current pool
   * statistics.
   */
  PoolStats<T> getStats();
}

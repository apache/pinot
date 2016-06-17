/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.metrics;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;


/**
 * Aggregated Histogram which aggregates counters.
 *  This class supports multi-level aggregations of counters
 *
 *
 */
public class AggregatedCounter implements Metric {
  // Container of inner meters
  private final CopyOnWriteArrayList<Metric> _counters = new CopyOnWriteArrayList<Metric>();

  private static final long DEFAULT_REFRESH_MS = 60 * 1000L; // 1 minute

  // Refresh Delay config
  private final long _refreshMs;

  // Last Refreshed timestamp
  private long _lastRefreshedTime;

  // Count of entries
  private volatile long _count;

  @Override
  public <T> void processWith(MetricProcessor<T> processor, MetricName name, T context) throws Exception {
    for (Metric c : _counters)
      c.processWith(processor, name, context);
  }

  public AggregatedCounter() {
    _refreshMs = DEFAULT_REFRESH_MS;
  }

  public AggregatedCounter(long refreshMs) {
    _refreshMs = refreshMs;
  }

  /**
   * Add Collection of metrics to be aggregated
   * @param counters collection of metrics to be aggregated
   * @return this instance
   */
  public AggregatedCounter addAll(Collection<? extends Metric> counters) {
    _counters.addAll(counters);
    return this;
  }

  /**
   * Add a metric to be aggregated
   * @param counter
   * @return this instance
   */
  public AggregatedCounter add(Metric counter) {
    _counters.add(counter);
    return this;
  }

  /**
   * Remove a metric which was already added
   * @param counter metric to be removed
   * @return true if the metric was present in the list
   */
  public boolean remove(Metric counter) {
    return _counters.remove(counter);
  }

  /**
   * Returns the counter's current value.
   *
   * @return the counter's current value
   */
  public long count() {
    refreshIfElapsed();
    return _count;
  }

  /**
   * Check elapsed time since last refresh and only refresh if time difference is
   * greater than threshold.
   */
  private void refreshIfElapsed() {
    long currentTime = System.currentTimeMillis();
    if (currentTime - _lastRefreshedTime > _refreshMs && !_counters.isEmpty()) {
      refresh();
      _lastRefreshedTime = currentTime;
    }
  }

  /**
   * Update counter from underlying counters.
   */
  public void refresh() {
    long count = 0;
    for (Metric m : _counters) {
      if (m instanceof Counter) {
        count += ((Counter) m).count();
      } else if (m instanceof AggregatedCounter) {
        count += ((AggregatedCounter) m).count();
      }
    }
    _count = count;
  }
}

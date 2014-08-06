package com.linkedin.pinot.common.metrics;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.yammer.metrics.core.Gauge;


/**
 * An aggregated gauge that provides an average among the underlying gauges. You can have
 * multi-level aggregations of gauges (long types)
 * 
 * @author bvaradar
 *
 * @param <T>
 */
public class AggregatedLongGauge<T extends Number, V extends Gauge<T>> extends Gauge<Long> {
  // Container of inner meters
  private final List<Gauge<T>> _gauges = new CopyOnWriteArrayList<Gauge<T>>();

  private final long DEFAULT_REFRESH_MS = 60 * 1000; // 1 minute

  // Refresh Delay config
  private final long _refreshMs;

  // Last Refreshed timestamp
  private volatile long _lastRefreshedTime;

  // The mean instantaneous value
  private volatile long _value;

  public AggregatedLongGauge(long refreshMs) {
    _refreshMs = refreshMs;
  }

  public AggregatedLongGauge() {
    _refreshMs = DEFAULT_REFRESH_MS;
  }

  /**
   * Add Collection of metrics to be aggregated
   * @param counters collection of metrics to be aggregated
   * @return this instance
   */
  public AggregatedLongGauge<T, V> addAll(Collection<Gauge<T>> gauges) {
    _gauges.addAll(gauges);
    return this;
  }

  /**
   * Add a metric to be aggregated
   * @param counter
   * @return this instance
   */
  public AggregatedLongGauge<T, V> add(Gauge<T> gauge) {
    _gauges.add(gauge);
    return this;
  }

  /**
   * Remove a metric which was already added
   * @param counter metric to be removed
   * @return true if the metric was present in the list
   */
  public boolean remove(Gauge<T> gauge) {
    return _gauges.remove(gauge);
  }

  /**
   * Check elapsed time since last refresh and only refresh if time difference is
   * greater than threshold.
   */
  private void refreshIfElapsed() {
    long currentTime = System.currentTimeMillis();
    if (currentTime - _lastRefreshedTime > _refreshMs && !_gauges.isEmpty()) {
      refresh();
      _lastRefreshedTime = currentTime;
    }
  }

  public void refresh() {
    long sum = 0;
    for (Gauge<T> gauge : _gauges) {
      sum += gauge.value().longValue();
    }
    _value = sum / _gauges.size();
  }

  @Override
  public Long value() {
    refreshIfElapsed();
    return _value;
  }
}

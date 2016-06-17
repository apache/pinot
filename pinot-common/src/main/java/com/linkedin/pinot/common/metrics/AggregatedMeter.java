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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Stoppable;


/**
 * Aggregated Meter Stats to consolidate metrics from across consolidated meters. We can have multi-level
 * aggregation with this class. One example is
 *  You have a JVM wide aggregated metric for ClientStats and a peer specific clientStats
 *  ( client objects connection to the same server) and individual metrics.
 *
 *  We have refreshMs that will throttle the aggregation frequency to 1 minute (by default). The refresh happens
 *  in the same thread which called the metric method.
 *
 */
public class AggregatedMeter<T extends Metered & Stoppable> implements Metered, Stoppable {

  public static final long SECONDS_IN_ONE_MIN = 60;
  public static final long SECONDS_IN_FIVE_MIN = SECONDS_IN_ONE_MIN * 5;
  public static final long SECONDS_IN_FIFTEEN_MIN = SECONDS_IN_ONE_MIN * 15;

  // Container of inner meters
  private final List<T> _meters = new CopyOnWriteArrayList<T>();

  private static final long DEFAULT_REFRESH_MS = 60 * 1000; // 1 minute

  // Refresh Delay config
  private final long _refreshMs;

  // Last Refreshed timestamp
  private volatile long _lastRefreshedTime;

  // Count of entries
  private volatile long _count;

  // Rates
  private volatile double _oneMinRate;
  private volatile double _fiveMinRate;
  private volatile double _fifteenMinRate;
  private volatile double _meanRate;

  public AggregatedMeter() {
    _refreshMs = DEFAULT_REFRESH_MS;
  }

  public AggregatedMeter(long refreshMs) {
    _refreshMs = refreshMs;
  }

  /**
   * Add Collection of metrics to be aggregated
   * @return this instance
   */
  public AggregatedMeter<T> addAll(Collection<T> meters) {
    _meters.addAll(meters);
    return this;
  }

  /**
   * Add a metric to be aggregated
   * @return this instance
   */
  public AggregatedMeter<T> add(T meter) {
    _meters.add(meter);
    return this;
  }

  /**
   * Remove a metric which was already added
   * @return true if the metric was present in the list
   */
  public boolean remove(T meter) {
    return _meters.remove(meter);
  }

  @Override
  public void stop() {
    for (T m : _meters)
      m.stop();
  }

  @Override
  public <T2> void processWith(MetricProcessor<T2> processor, MetricName name, T2 context) throws Exception {
    for (T m : _meters) {
      m.processWith(processor, name, context);
    }
  }

  /**
   * Check elapsed time since last refresh and only refresh if time difference is
   * greater than threshold.
   */
  private void refreshIfElapsed() {
    long currentTime = System.currentTimeMillis();
    if (currentTime - _lastRefreshedTime > _refreshMs && !_meters.isEmpty()) {
      refresh();
      _lastRefreshedTime = currentTime;
    }
  }

  public void refresh() {
    // Refresh 1 min
    long oneMinSum = 0;
    long fiveMinSum = 0;
    long fifteenMinSum = 0;
    long meanSum = 0;
    int count = _meters.size();
    _count = 0;
    for (T m : _meters) {
      oneMinSum += m.oneMinuteRate() * SECONDS_IN_ONE_MIN;
      fiveMinSum += m.fiveMinuteRate() * SECONDS_IN_FIVE_MIN;
      fifteenMinSum += m.fifteenMinuteRate() * SECONDS_IN_FIFTEEN_MIN;
      meanSum += m.meanRate() * m.count();
      _count += m.count();
    }

    _oneMinRate = oneMinSum / (count * SECONDS_IN_ONE_MIN * 1.0);
    _fiveMinRate = fiveMinSum / (count * SECONDS_IN_FIVE_MIN * 1.0);
    _fifteenMinRate = fifteenMinSum / (count * SECONDS_IN_FIFTEEN_MIN * 1.0);
    _meanRate = meanSum / _count;
  }

  @Override
  public TimeUnit rateUnit() {
    if (_meters.isEmpty())
      return null;
    return _meters.get(0).rateUnit();
  }

  @Override
  public String eventType() {
    if (_meters.isEmpty())
      return null;
    return _meters.get(0).eventType();
  }

  @Override
  public long count() {
    refreshIfElapsed();
    return _count;
  }

  @Override
  public double fifteenMinuteRate() {
    refreshIfElapsed();
    return _fifteenMinRate;
  }

  @Override
  public double fiveMinuteRate() {
    refreshIfElapsed();
    return _fiveMinRate;
  }

  @Override
  public double meanRate() {
    refreshIfElapsed();
    return _meanRate;
  }

  @Override
  public double oneMinuteRate() {
    refreshIfElapsed();
    return _oneMinRate;
  }
}

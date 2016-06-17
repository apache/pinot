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

import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.stats.Snapshot;


/**
 *
 * Latency Metric Snapshot constructed from Histogram ( or AggregatedHistogram)
 * Provides a snapshot of commonly used latency numbers.
 *
 *
 * @param <T> Histogram
 */
public class LatencyMetric<T extends Sampling & Summarizable> {

  private final double _mean;
  private final double _min;
  private final double _max;
  private final double _percentile95;
  private final double _percentile99;
  private final double _percentile999;
  private final T _histogram;

  public LatencyMetric(T h) {
    Snapshot s = h.getSnapshot();
    _min = h.min();
    _max = h.max();
    _mean = h.mean();
    if (null != s) {
      _percentile95 = s.get95thPercentile();
      _percentile99 = s.get99thPercentile();
      _percentile999 = s.get999thPercentile();
    } else {
      _percentile95 = -1;
      _percentile99 = -1;
      _percentile999 = -1;
    }
    _histogram = h;
  }

  public double getMean() {
    return _mean;
  }

  public double getMin() {
    return _min;
  }

  public double getMax() {
    return _max;
  }

  public double getPercentile95() {
    return _percentile95;
  }

  public double getPercentile99() {
    return _percentile99;
  }

  public double getPercentile999() {
    return _percentile999;
  }

  public T getHistogram() {
    return _histogram;
  }

  @Override
  public String toString() {
    return "LatencyMetric [_mean=" + _mean + ", _min=" + _min + ", _max=" + _max + ", _percentile95=" + _percentile95
        + ", _percentile99=" + _percentile99 + ", _percentile999=" + _percentile999 + ", _histogram=" + _histogram
        + "]";
  }

}

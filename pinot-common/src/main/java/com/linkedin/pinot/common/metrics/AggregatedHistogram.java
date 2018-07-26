/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.stats.Snapshot;


/**
 *
 * Aggregated Histogram which aggregates and provides Uniform Sampling Histograms.
 * We can have multi-level aggregations.
 *
 *
 */
public class AggregatedHistogram<T extends Sampling> implements Sampling, Summarizable, Metric {

  private final List<T> _histograms = new CopyOnWriteArrayList<T>();

  private static final long DEFAULT_REFRESH_MS = 60 * 1000L; // 1 minute

  // Refresh Delay config
  private final long _refreshMs;

  // Last Refreshed timestamp
  private volatile long _lastRefreshedTime;

  // Sampling stats
  private volatile double _max;
  private volatile double _min;
  private volatile double _mean;
  private volatile double _stdDev;
  private volatile double _sum;

  //Sanpshot
  private volatile Snapshot _snapshot;

  public AggregatedHistogram(long refreshMs) {
    _refreshMs = refreshMs;
  }

  public AggregatedHistogram() {
    _refreshMs = DEFAULT_REFRESH_MS;
  }

  /**
   * Add Collection of metrics to be aggregated
   * @return this instance
   */
  public AggregatedHistogram<T> addAll(Collection<T> histograms) {
    _histograms.addAll(histograms);
    return this;
  }

  /**
   * Add a metric to be aggregated
   * @return this instance
   */
  public AggregatedHistogram<T> add(T histogram) {
    _histograms.add(histogram);
    return this;
  }

  /**
   * Remove a metric which was already added
   * @return true if the metric was present in the list
   */
  public boolean remove(T histogram) {
    return _histograms.remove(histogram);
  }

  /**
   * Check elapsed time since last refresh and only refresh if time difference is
   * greater than threshold.
   */
  private void refreshIfElapsed() {
    long currentTime = System.currentTimeMillis();
    if (currentTime - _lastRefreshedTime > _refreshMs && !_histograms.isEmpty()) {
      refresh();
      _lastRefreshedTime = currentTime;
    }
  }

  /**
   * update all stats using underlying histograms
   */
  public void refresh() {
    List<Double> values = new ArrayList<Double>();
    _min = Double.MAX_VALUE;
    _max = Double.MIN_VALUE;
    _sum = 0;
    double meanSum = 0.0;
    for (T hist : _histograms) {
      if (hist instanceof Histogram) {
        Histogram h = (Histogram) hist;
        _min = Math.min(_min, h.min());
        _max = Math.max(_max, h.max());
        _sum += h.sum();
        meanSum += h.mean();
      } else {
        AggregatedHistogram<Sampling> h = (AggregatedHistogram<Sampling>) hist;
        _min = Math.min(_min, h.min());
        _max = Math.max(_max, h.max());
        _sum += h.sum();
        meanSum += h.mean();
      }
      double[] val = hist.getSnapshot().getValues();
      for (double d : val) {
        values.add(d);
      }
    }

    if (!_histograms.isEmpty()) {
      _mean = meanSum / _histograms.size();
    }

    if (!values.isEmpty()) {

      double[] vals = new double[values.size()];

      int i = 0;
      for (Double d : values) {
        vals[i++] = d;
      }

      _snapshot = new Snapshot(vals);
    }
  }

  @Override
  public double max() {
    refreshIfElapsed();
    return _max;
  }

  @Override
  public double min() {
    refreshIfElapsed();
    return _min;
  }

  @Override
  public double mean() {
    refreshIfElapsed();
    return _mean;
  }

  @Override
  public double stdDev() {
    refreshIfElapsed();
    return _stdDev;
  }

  @Override
  public double sum() {
    refreshIfElapsed();
    return _sum;
  }

  @Override
  public Snapshot getSnapshot() {
    refreshIfElapsed();
    return _snapshot;
  }

  @Override
  public <T2> void processWith(MetricProcessor<T2> processor, MetricName name, T2 context) throws Exception {
    for (T h : _histograms) {
      if (h instanceof Metric) {
        ((Metric) h).processWith(processor, name, context);
      }
    }
  }

}

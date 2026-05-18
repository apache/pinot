/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.metrics.yammer;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistryListener;
import com.yammer.metrics.core.Timer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.pinot.common.metrics.MetricsInspector;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;


/**
 * Yammer-backed implementation of {@link MetricsInspector}. Uses Yammer's {@link MetricsRegistryListener} to capture
 * metric registrations and provides timer-sum / metered-count readings via Yammer's {@link MetricProcessor}.
 */
public class YammerMetricsInspector extends MetricsInspector {
  private final Map<MetricName, Metric> _metricMap = new HashMap<>();
  private volatile MetricName _lastMetric;

  public YammerMetricsInspector(PinotMetricsRegistry registry) {
    registry.addListener(() -> new MetricsRegistryListener() {
      @Override
      public void onMetricAdded(MetricName metricName, Metric metric) {
        _metricMap.put(metricName, metric);
        _lastMetric = metricName;
      }

      @Override
      public void onMetricRemoved(MetricName metricName) {
        _metricMap.remove(metricName);
      }
    });
  }

  @Override
  public PinotMetricName lastMetric() {
    return _lastMetric == null ? null : new YammerMetricName(_lastMetric);
  }

  @Override
  public long getTimerSumMs(PinotMetricName name) {
    return access(name, m -> m._timer == null ? null : (long) m._timer.sum());
  }

  @Override
  public long getMeteredCount(PinotMetricName name) {
    return access(name, m -> m._metered == null ? null : m._metered.count());
  }

  private <T> T access(PinotMetricName name, Function<MetricAccessor, T> property) {
    MetricName inner = ((YammerMetricName) name).getMetricName();
    Metric metric = _metricMap.get(inner);
    if (metric == null) {
      throw new IllegalArgumentException("Metric not found: " + inner);
    }
    MetricAccessor accessor = new MetricAccessor();
    try {
      metric.processWith(accessor, null, null);
    } catch (Exception e) {
      throw new IllegalStateException("Unexpected error processing metric: " + metric, e);
    }
    T result = property.apply(accessor);
    if (result == null) {
      throw new IllegalArgumentException("Requested metric type not found in metric [" + inner.getName() + "]");
    }
    return result;
  }

  private static class MetricAccessor implements MetricProcessor<Void> {
    public Metered _metered;
    public Counter _counter;
    public Histogram _histogram;
    public Timer _timer;
    public Gauge<?> _gauge;

    @Override
    public void processMeter(MetricName n, Metered metered, Void v) {
      _metered = metered;
    }

    @Override
    public void processCounter(MetricName n, Counter counter, Void v) {
      _counter = counter;
    }

    @Override
    public void processHistogram(MetricName n, Histogram histogram, Void v) {
      _histogram = histogram;
    }

    @Override
    public void processTimer(MetricName n, Timer timer, Void v) {
      _timer = timer;
      // Timer also implements Metered.
      _metered = timer;
    }

    @Override
    public void processGauge(MetricName metricName, Gauge<?> gauge, Void v) {
      _gauge = gauge;
    }
  }
}

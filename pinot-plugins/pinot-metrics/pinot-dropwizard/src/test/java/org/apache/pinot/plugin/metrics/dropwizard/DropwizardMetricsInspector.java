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
package org.apache.pinot.plugin.metrics.dropwizard;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Timer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.MetricsInspector;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;


/**
 * Dropwizard-backed implementation of {@link MetricsInspector}.
 */
public class DropwizardMetricsInspector extends MetricsInspector {
  private final Map<String, Metric> _metricMap = new HashMap<>();
  private volatile String _lastMetric;

  public DropwizardMetricsInspector(PinotMetricsRegistry registry) {
    registry.addListener(new DropwizardMetricsRegistryListener(new MetricRegistryListener() {
      @Override
      public void onGaugeAdded(String name, Gauge<?> metric) {
        record(name, metric);
      }

      @Override
      public void onGaugeRemoved(String name) {
        _metricMap.remove(name);
      }

      @Override
      public void onCounterAdded(String name, Counter metric) {
        record(name, metric);
      }

      @Override
      public void onCounterRemoved(String name) {
        _metricMap.remove(name);
      }

      @Override
      public void onHistogramAdded(String name, Histogram metric) {
        record(name, metric);
      }

      @Override
      public void onHistogramRemoved(String name) {
        _metricMap.remove(name);
      }

      @Override
      public void onMeterAdded(String name, Meter metric) {
        record(name, metric);
      }

      @Override
      public void onMeterRemoved(String name) {
        _metricMap.remove(name);
      }

      @Override
      public void onTimerAdded(String name, Timer metric) {
        record(name, metric);
      }

      @Override
      public void onTimerRemoved(String name) {
        _metricMap.remove(name);
      }

      private void record(String name, Metric metric) {
        _metricMap.put(name, metric);
        _lastMetric = name;
      }
    }));
  }

  @Override
  public PinotMetricName lastMetric() {
    return _lastMetric == null ? null : new DropwizardMetricName(_lastMetric);
  }

  @Override
  public long getTimerSumMs(PinotMetricName name) {
    Metric metric = _metricMap.get(((DropwizardMetricName) name).getMetricName());
    if (!(metric instanceof Timer)) {
      throw new IllegalArgumentException("Not a timer metric: " + name);
    }
    // Dropwizard Timer has no sum() method — derive it from the snapshot's recorded values (in ns).
    // For tests the timer uses a SlidingTimeWindowArrayReservoir so every recorded value is present.
    long totalNanos = 0;
    for (long ns : ((Timer) metric).getSnapshot().getValues()) {
      totalNanos += ns;
    }
    return TimeUnit.NANOSECONDS.toMillis(totalNanos);
  }

  @Override
  public long getMeteredCount(PinotMetricName name) {
    Metric metric = _metricMap.get(((DropwizardMetricName) name).getMetricName());
    if (metric instanceof Meter) {
      return ((Meter) metric).getCount();
    }
    if (metric instanceof Timer) {
      return ((Timer) metric).getCount();
    }
    throw new IllegalArgumentException("Not a metered metric: " + name);
  }
}

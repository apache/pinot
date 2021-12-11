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

import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.metrics.PinotCounter;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotHistogram;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetric;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.metrics.PinotMetricsRegistryListener;
import org.apache.pinot.spi.metrics.PinotTimer;


public class YammerMetricsRegistry implements PinotMetricsRegistry {
  MetricsRegistry _metricsRegistry;

  public YammerMetricsRegistry() {
    _metricsRegistry = new MetricsRegistry();
  }

  public YammerMetricsRegistry(Clock clock) {
    _metricsRegistry = new MetricsRegistry(clock);
  }

  @Override
  public void removeMetric(PinotMetricName name) {
    _metricsRegistry.removeMetric((MetricName) name.getMetricName());
  }

  @Override
  public <T> PinotGauge<T> newGauge(PinotMetricName name, PinotGauge<T> gauge) {
    return new YammerGauge<T>(
        _metricsRegistry.newGauge((MetricName) name.getMetricName(), (Gauge<T>) gauge.getGauge()));
  }

  @Override
  public PinotMeter newMeter(PinotMetricName name, String eventType, TimeUnit unit) {
    return new YammerMeter(_metricsRegistry.newMeter((MetricName) name.getMetricName(), eventType, unit));
  }

  @Override
  public PinotCounter newCounter(PinotMetricName name) {
    return new YammerCounter(_metricsRegistry.newCounter((MetricName) name.getMetricName()));
  }

  @Override
  public PinotTimer newTimer(PinotMetricName name, TimeUnit durationUnit, TimeUnit rateUnit) {
    return new YammerTimer(_metricsRegistry.newTimer((MetricName) name.getMetricName(), durationUnit, rateUnit));
  }

  @Override
  public PinotHistogram newHistogram(PinotMetricName name, boolean biased) {
    return null;
  }

  @Override
  public Map<PinotMetricName, PinotMetric> allMetrics() {
    Map<MetricName, Metric> yammerMetrics = _metricsRegistry.allMetrics();
    Map<PinotMetricName, PinotMetric> allMetrics = new HashMap<>();
    for (Map.Entry<MetricName, Metric> entry : yammerMetrics.entrySet()) {
      allMetrics.put(new YammerMetricName(entry.getKey()), new YammerMetric(entry.getValue()));
    }
    return allMetrics;
  }

  @Override
  public void addListener(PinotMetricsRegistryListener listener) {
    _metricsRegistry.addListener((MetricsRegistryListener) listener.getMetricsRegistryListener());
  }

  @Override
  public Object getMetricsRegistry() {
    return _metricsRegistry;
  }

  @Override
  public void shutdown() {
    _metricsRegistry.shutdown();
  }
}

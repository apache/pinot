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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
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


public class DropwizardMetricsRegistry implements PinotMetricsRegistry {
  MetricRegistry _metricRegistry;

  public DropwizardMetricsRegistry() {
    _metricRegistry = new MetricRegistry();
  }

  @Override
  public void removeMetric(PinotMetricName name) {
    _metricRegistry.remove(name.getMetricName().toString());
  }

  @Override
  public <T> PinotGauge<T> newGauge(PinotMetricName name, final PinotGauge<T> gauge) {
    final String metricName = name.getMetricName().toString();
    return (gauge == null) ? new DropwizardGauge<T>((DropwizardSettableGauge) _metricRegistry.gauge(metricName))
        : new DropwizardGauge<T>(_metricRegistry.gauge(metricName, () -> (DropwizardSettableGauge) gauge.getGauge()));
  }

  @Override
  public PinotMeter newMeter(PinotMetricName name, String eventType, TimeUnit unit) {
    return new DropwizardMeter(_metricRegistry.meter(name.getMetricName().toString()));
  }

  @Override
  public PinotCounter newCounter(PinotMetricName name) {
    return new DropwizardCounter(_metricRegistry.counter(name.getMetricName().toString()));
  }

  @Override
  public PinotTimer newTimer(PinotMetricName name, TimeUnit durationUnit, TimeUnit rateUnit) {
    Timer timer = _metricRegistry.timer(name.getMetricName().toString(),
        () -> new Timer(new SlidingTimeWindowArrayReservoir(15, TimeUnit.MINUTES)));
    return new DropwizardTimer(timer);
  }

  @Override
  public PinotHistogram newHistogram(PinotMetricName name, boolean biased) {
    Histogram histogram = _metricRegistry.histogram(name.getMetricName().toString(),
        () -> new Histogram(new SlidingTimeWindowArrayReservoir(15, TimeUnit.MINUTES)));
    return new DropWizardHistogram(histogram);
  }

  @Override
  public Map<PinotMetricName, PinotMetric> allMetrics() {
    Map<String, Metric> dropwizardMetrics = _metricRegistry.getMetrics();
    Map<PinotMetricName, PinotMetric> allMetrics = new HashMap<>();
    for (Map.Entry<String, Metric> entry : dropwizardMetrics.entrySet()) {
      allMetrics.put(new DropwizardMetricName(entry.getKey()), new DropwizardMetric(entry.getValue()));
    }
    return allMetrics;
  }

  @Override
  public void addListener(PinotMetricsRegistryListener listener) {
    _metricRegistry.addListener((MetricRegistryListener) listener.getMetricsRegistryListener());
  }

  @Override
  public Object getMetricsRegistry() {
    return _metricRegistry;
  }

  @Override
  public void shutdown() {
  }
}

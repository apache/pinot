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
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
  final MetricRegistry _metricRegistry;
  private final Map<String, MetricMetadata> _metricMetadata = new ConcurrentHashMap<>();
  private final Set<DropwizardJmxReporter> _reporters = new HashSet<>();
  private boolean _shutdown;

  public DropwizardMetricsRegistry() {
    _metricRegistry = new MetricRegistry();
  }

  @Override
  public synchronized void removeMetric(PinotMetricName name) {
    String metricName = name.getMetricName().toString();
    _metricRegistry.remove(metricName);
    _metricMetadata.remove(metricName);
  }

  @Override
  // MetricRegistry erases gauge value types; Pinot's typed lookup retains the original registration's type.
  @SuppressWarnings("unchecked")
  public <T> PinotGauge<T> newGauge(PinotMetricName name, final PinotGauge<T> gauge) {
    Metric metric = getRegisteredMetric(name);
    if (metric instanceof DropwizardSettableGauge) {
      return new DropwizardGauge<>((DropwizardSettableGauge<T>) metric);
    }
    synchronized (this) {
      registerMetadata(name, null, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
      final String metricName = name.getMetricName().toString();
      return (gauge == null) ? new DropwizardGauge<T>((DropwizardSettableGauge) _metricRegistry.gauge(metricName))
          : new DropwizardGauge<T>(_metricRegistry.gauge(metricName, () -> (DropwizardSettableGauge) gauge.getGauge()));
    }
  }

  @Override
  public PinotMeter newMeter(PinotMetricName name, String eventType, TimeUnit unit) {
    Metric metric = getRegisteredMetric(name);
    if (metric instanceof Meter) {
      return new DropwizardMeter((Meter) metric);
    }
    synchronized (this) {
      registerMetadata(name, eventType, unit, TimeUnit.MILLISECONDS);
      return new DropwizardMeter(_metricRegistry.meter(name.getMetricName().toString()));
    }
  }

  @Override
  public PinotCounter newCounter(PinotMetricName name) {
    Metric metric = getRegisteredMetric(name);
    if (metric instanceof Counter) {
      return new DropwizardCounter((Counter) metric);
    }
    synchronized (this) {
      registerMetadata(name, null, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
      return new DropwizardCounter(_metricRegistry.counter(name.getMetricName().toString()));
    }
  }

  @Override
  public PinotTimer newTimer(PinotMetricName name, TimeUnit durationUnit, TimeUnit rateUnit) {
    Metric metric = getRegisteredMetric(name);
    if (metric instanceof Timer) {
      return new DropwizardTimer((Timer) metric);
    }
    synchronized (this) {
      registerMetadata(name, "calls", rateUnit, durationUnit);
      Timer timer = _metricRegistry.timer(name.getMetricName().toString(),
          () -> new Timer(new SlidingTimeWindowArrayReservoir(15, TimeUnit.MINUTES)));
      return new DropwizardTimer(timer);
    }
  }

  @Override
  public PinotHistogram newHistogram(PinotMetricName name, boolean biased) {
    Metric metric = getRegisteredMetric(name);
    if (metric instanceof Histogram) {
      return new DropWizardHistogram((Histogram) metric);
    }
    synchronized (this) {
      registerMetadata(name, null, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
      Histogram histogram = _metricRegistry.histogram(name.getMetricName().toString(),
          () -> new Histogram(new SlidingTimeWindowArrayReservoir(15, TimeUnit.MINUTES)));
      return new DropWizardHistogram(histogram);
    }
  }

  @Override
  public Map<PinotMetricName, PinotMetric> allMetrics() {
    Map<String, Metric> dropwizardMetrics = _metricRegistry.getMetrics();
    Map<PinotMetricName, PinotMetric> allMetrics = new HashMap<>();
    for (Map.Entry<String, Metric> entry : dropwizardMetrics.entrySet()) {
      MetricMetadata metadata = _metricMetadata.get(entry.getKey());
      DropwizardMetricName name = metadata == null ? new DropwizardMetricName(entry.getKey()) : metadata._name;
      allMetrics.put(name, new DropwizardMetric(entry.getValue()));
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
  public synchronized void shutdown() {
    _shutdown = true;
    for (DropwizardJmxReporter reporter : List.copyOf(_reporters)) {
      reporter.close();
    }
    _metricRegistry.removeMatching(MetricFilter.ALL);
    _metricMetadata.clear();
  }

  private void registerMetadata(PinotMetricName name, String eventType, TimeUnit rateUnit, TimeUnit durationUnit) {
    String metricName = name.getMetricName().toString();
    DropwizardMetricName dropwizardName =
        name instanceof DropwizardMetricName ? (DropwizardMetricName) name : new DropwizardMetricName(metricName);
    // Publish before MetricRegistry invokes its listeners, and retain the first registration's units.
    _metricMetadata.putIfAbsent(metricName, new MetricMetadata(dropwizardName, eventType, rateUnit, durationUnit));
  }

  private Metric getRegisteredMetric(PinotMetricName name) {
    String metricName = name.getMetricName().toString();
    // Existing updates avoid the registration monitor; return the captured metric even if it is removed concurrently.
    return _metricMetadata.containsKey(metricName) ? _metricRegistry.getMetrics().get(metricName) : null;
  }

  MetricMetadata getMetricMetadata(String metricName) {
    return _metricMetadata.get(metricName);
  }

  synchronized boolean registerReporter(DropwizardJmxReporter reporter) {
    return !_shutdown && _reporters.add(reporter);
  }

  synchronized void unregisterReporter(DropwizardJmxReporter reporter) {
    _reporters.remove(reporter);
  }

  /// Immutable registration metadata used to preserve the JMX name and units independently of Dropwizard's raw metrics.
  static final class MetricMetadata {
    final DropwizardMetricName _name;
    final String _eventType;
    final TimeUnit _rateUnit;
    final TimeUnit _durationUnit;

    MetricMetadata(DropwizardMetricName name, String eventType, TimeUnit rateUnit, TimeUnit durationUnit) {
      _name = name;
      _eventType = eventType;
      _rateUnit = rateUnit;
      _durationUnit = durationUnit;
    }
  }
}

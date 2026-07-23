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
package org.apache.pinot.plugin.metrics.micrometer;

import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import org.apache.pinot.spi.annotations.metrics.MetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotJmxReporter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.plugin.PluginManager;


/// Genuinely-native Micrometer implementation of {@link PinotMetricsFactory}.
///
/// <p>Builds a single {@link io.micrometer.core.instrument.composite.CompositeMeterRegistry} with config-toggled
/// export legs (see {@link MicrometerMetricsRegistry}):
/// <ul>
///   <li><b>Prometheus leg (opt-in):</b> a Micrometer {@code PrometheusMeterRegistry}, optionally served over
///       HTTP.</li>
///   <li><b>JMX leg (opt-in):</b> a Micrometer {@code JmxMeterRegistry} exposing meters as MBeans under
///       Micrometer-native names (NOT Yammer-compatible).</li>
/// </ul>
///
/// <p>The exported metric names/resolutions follow Micrometer conventions and intentionally differ from the legacy
/// Yammer/Dropwizard JMX names. Customers needing the exact legacy names use the separate compound registry.
///
/// <p>Config properties (prefix {@code pinot.metrics.micrometer}):
/// <ul>
///   <li>{@code .prometheus.enabled} — enable the Prometheus export leg (default: {@code false})</li>
///   <li>{@code .prometheus.port} — scrape-endpoint HTTP port; {@code 0} = no server (default: {@code 0})</li>
///   <li>{@code .prometheus.path} — HTTP path for the scrape endpoint (default: {@code /metrics})</li>
///   <li>{@code .jmx.enabled} — enable the (Micrometer-native) JMX export leg (default: {@code false})</li>
///   <li>{@code .namer.className} — pluggable {@link MicrometerMetricNamer} controlling the exported name shape
///       (default {@link DefaultMicrometerMetricNamer}, which drops the metrics-class prefix for Yammer-closer
///       names)</li>
///   <li>{@code .percentiles} — comma-separated client-side percentiles published for timers and distribution
///       summaries (default {@code 0.5,0.75,0.95,0.98,0.99,0.999}, matching the legacy Yammer set; empty = none)</li>
/// </ul>
@AutoService(PinotMetricsFactory.class)
@MetricsFactory
public class MicrometerMetricsFactory implements PinotMetricsFactory {
  public static final String PROMETHEUS_ENABLED_PROP = "pinot.metrics.micrometer.prometheus.enabled";
  public static final String PROMETHEUS_PORT_PROP = "pinot.metrics.micrometer.prometheus.port";
  public static final String PROMETHEUS_PATH_PROP = "pinot.metrics.micrometer.prometheus.path";
  public static final String JMX_ENABLED_PROP = "pinot.metrics.micrometer.jmx.enabled";
  public static final String NAMER_CLASS_PROP = "pinot.metrics.micrometer.namer.className";
  public static final String PERCENTILES_PROP = "pinot.metrics.micrometer.percentiles";

  public static final boolean DEFAULT_PROMETHEUS_ENABLED = false;
  public static final int DEFAULT_PROMETHEUS_PORT = 0;
  public static final String DEFAULT_PROMETHEUS_PATH = "/metrics";
  public static final boolean DEFAULT_JMX_ENABLED = false;
  // Matches the percentiles the legacy Yammer/Dropwizard timer MBean exposed (50/75/95/98/99/99.9), for closeness.
  public static final String DEFAULT_PERCENTILES = "0.5,0.75,0.95,0.98,0.99,0.999";

  // Written by init() and read by getPinotMetricsRegistry()/makePinotMetricName(), potentially from different
  // threads; volatile establishes the happens-before edge. The registry is eagerly defaulted (no legs) so callers
  // that never invoke init() (e.g. tests) still get a consistent instance; init() replaces it to apply config.
  private volatile MicrometerMetricsRegistry _pinotMetricsRegistry = new MicrometerMetricsRegistry();
  private volatile MicrometerMetricNamer _namer = new DefaultMicrometerMetricNamer();

  @Override
  public void init(PinotConfiguration metricsConfiguration) {
    _namer = loadNamer(metricsConfiguration);
    boolean prometheusEnabled = metricsConfiguration.getProperty(PROMETHEUS_ENABLED_PROP, DEFAULT_PROMETHEUS_ENABLED);
    int port = metricsConfiguration.getProperty(PROMETHEUS_PORT_PROP, DEFAULT_PROMETHEUS_PORT);
    String path = metricsConfiguration.getProperty(PROMETHEUS_PATH_PROP, DEFAULT_PROMETHEUS_PATH);
    boolean jmxEnabled = metricsConfiguration.getProperty(JMX_ENABLED_PROP, DEFAULT_JMX_ENABLED);
    double[] percentiles = parsePercentiles(metricsConfiguration.getProperty(PERCENTILES_PROP, DEFAULT_PERCENTILES));
    _pinotMetricsRegistry = new MicrometerMetricsRegistry(prometheusEnabled, port, path, jmxEnabled, percentiles);
  }

  private static MicrometerMetricNamer loadNamer(PinotConfiguration metricsConfiguration) {
    String className = metricsConfiguration.getProperty(NAMER_CLASS_PROP);
    if (className == null || className.isBlank()) {
      return new DefaultMicrometerMetricNamer();
    }
    try {
      return PluginManager.get().createInstance(className);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot instantiate MicrometerMetricNamer: " + className, e);
    }
  }

  /// Parses the comma-separated percentile config, validating each value is finite and in {@code (0, 1]} (Micrometer's
  /// domain). Fails fast on a bad value — a common mistake is the Yammer-style {@code 50,75,99} (which would parse to
  /// {@code 50.0, ...} and silently emit nonsensical quantile series) instead of {@code 0.5,0.75,0.99}.
  private static double[] parsePercentiles(String csv) {
    if (csv == null || csv.isBlank()) {
      return new double[0];
    }
    String[] tokens = csv.split(",");
    double[] parsed = new double[tokens.length];
    int count = 0;
    for (String token : tokens) {
      String trimmed = token.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      double value;
      try {
        value = Double.parseDouble(trimmed);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid value '" + trimmed + "' in " + PERCENTILES_PROP + " (expected values in (0, 1], e.g. 0.99)", e);
      }
      if (!Double.isFinite(value) || value <= 0.0 || value > 1.0) {
        throw new IllegalArgumentException(
            "Percentile '" + trimmed + "' in " + PERCENTILES_PROP + " must be in (0, 1] (e.g. 0.99, not 99)");
      }
      parsed[count++] = value;
    }
    return count == parsed.length ? parsed : Arrays.copyOf(parsed, count);
  }

  @Override
  public PinotMetricsRegistry getPinotMetricsRegistry() {
    return _pinotMetricsRegistry;
  }

  @Override
  public PinotMetricName makePinotMetricName(Class<?> klass, String name) {
    String meterName = _namer.meterName(klass, name);
    if (meterName == null) {
      throw new IllegalStateException(
          "MicrometerMetricNamer " + _namer.getClass().getName() + " returned null for " + klass.getSimpleName() + "."
              + name);
    }
    return new MicrometerMetricName(meterName);
  }

  /// Tag-first override: applies the namer to the dimension-free {@code baseName} so the Prometheus series name does
  /// not contain embedded table/type/partition segments, and attaches the tags for label-based export.
  @Override
  public PinotMetricName makePinotMetricName(Class<?> klass, String flatName, String baseName,
      Map<String, String> tags) {
    String meterName = _namer.meterName(klass, baseName);
    if (meterName == null) {
      throw new IllegalStateException(
          "MicrometerMetricNamer " + _namer.getClass().getName() + " returned null for " + klass.getSimpleName() + "."
              + baseName);
    }
    return new MicrometerMetricName(meterName, tags);
  }

  @Override
  public <T> PinotGauge<T> makePinotGauge(Function<Void, T> condition) {
    return new MicrometerGauge<>(condition);
  }

  /// The Micrometer {@code JmxMeterRegistry} exports continuously once added to the composite (when the JMX leg is
  /// enabled), so there is nothing for a separate reporter to start: a no-op reporter is returned whose
  /// {@link PinotJmxReporter#start()} does nothing.
  @Override
  public PinotJmxReporter makePinotJmxReporter(PinotMetricsRegistry metricsRegistry) {
    return () -> {
    };
  }

  @Override
  public String getMetricsFactoryName() {
    return "Micrometer";
  }
}

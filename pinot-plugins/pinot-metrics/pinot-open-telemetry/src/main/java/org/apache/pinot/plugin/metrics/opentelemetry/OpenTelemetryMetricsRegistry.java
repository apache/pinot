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
package org.apache.pinot.plugin.metrics.opentelemetry;

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.pinot.spi.metrics.PinotCounter;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotHistogram;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetric;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.metrics.PinotMetricsRegistryListener;
import org.apache.pinot.spi.metrics.PinotTimer;

/**
 * OpenTelemetryMetricsRegistry is the implementation of {@link PinotMetricsRegistry} for OpenTelemetry.
 */
public class OpenTelemetryMetricsRegistry implements PinotMetricsRegistry {
  private static final Map<PinotMetricName, OpenTelemetryCounter> PINOT_COUNTER_MAP = new ConcurrentHashMap<>();
  private static final Map<PinotMetricName, OpenTelemetryMeter> PINOT_METER_MAP = new ConcurrentHashMap<>();
  private static final Map<PinotMetricName, OpenTelemetryLongGauge> PINOT_LONG_GAUGE_MAP = new ConcurrentHashMap<>();
  private static final Map<PinotMetricName, OpenTelemetryDoubleGauge> PINOT_DOUBLE_GAUGE_MAP =
      new ConcurrentHashMap<>();
  private static final Map<PinotMetricName, OpenTelemetryHistogram> PINOT_HISTOGRAM_MAP = new ConcurrentHashMap<>();
  private static final Map<PinotMetricName, OpenTelemetryTimer> PINOT_TIMER_MAP = new ConcurrentHashMap<>();
  private static final List<OpenTelemetryMetricsRegistryListener> LISTENERS = new LinkedList<>();

  // _otelMeterProvider is the metric provider for OpenTelemetry metrics.
  // The naming is a little confusing here, PinotMeter is actually a Counter in OpenTelemetry. While in OpenTelemetry,
  // the terminology of Meter is used to represent a broader category of metrics, including counters, gauges, and
  // histograms.
  public static Meter _otelMeterProvider;

  public OpenTelemetryMetricsRegistry() {
    init(OpenTelemetryHttpReporter.DEFAULT_HTTP_METRIC_EXPORTER,
        OpenTelemetryHttpReporter.DEFAULT_EXPORT_INTERVAL_SECONDS);
  }

  public static void init(OtlpHttpMetricExporter otlpHttpMetricExporter, int exportIntervalInSeconds) {
    // Configures the OpenTelemetry SDK Meter Provider with the given OTLP HTTP Metric Exporter and set export interval.
    SdkMeterProvider sdkMeterProvider = SdkMeterProvider
        .builder()
        .registerMetricReader(PeriodicMetricReader.builder(otlpHttpMetricExporter)
            .setInterval(Duration.ofSeconds(exportIntervalInSeconds))
            .build())
        .build();
    // Initialize the static _otelMeterProvider
    _otelMeterProvider = sdkMeterProvider.get(OpenTelemetryUtil.OTEL_METRICS_SCOPE);
  }

  public static Meter getOtelMeterProvider() {
    return _otelMeterProvider;
  }

  @Override
  public void removeMetric(PinotMetricName pinotMetricname) {
    PINOT_COUNTER_MAP.remove(pinotMetricname);
    PINOT_METER_MAP.remove(pinotMetricname);
    PINOT_LONG_GAUGE_MAP.remove(pinotMetricname);
    PINOT_DOUBLE_GAUGE_MAP.remove(pinotMetricname);
    PINOT_HISTOGRAM_MAP.remove(pinotMetricname);
    PINOT_TIMER_MAP.remove(pinotMetricname);

    if (!LISTENERS.isEmpty()) {
      OpenTelemetryMetricName metricName = new OpenTelemetryMetricName(pinotMetricname.getMetricName().toString());
      for (OpenTelemetryMetricsRegistryListener listener : LISTENERS) {
        listener.onMetricRemoved(metricName);
      }
    }
  }

  @Override
  public <T> PinotGauge<T> newGauge(PinotMetricName pinotMetricname, PinotGauge<T> gauge) {
    OpenTelemetryMetricName metricName = new OpenTelemetryMetricName(pinotMetricname.getMetricName().toString());
    T value = gauge.value();

    if (value instanceof Integer || value instanceof Long) {
      return PINOT_LONG_GAUGE_MAP.computeIfAbsent(metricName, n -> {
        OpenTelemetryLongGauge<Long> otelLongGauge = SharedOtelMetricRegistry
            .getOrCreateOtelLongGauge(metricName, v -> (long) value);
        if (!LISTENERS.isEmpty()) {
          for (OpenTelemetryMetricsRegistryListener listener : LISTENERS) {
            listener.onMetricAdded(metricName, otelLongGauge);
          }
        }
        return otelLongGauge;
      });
    }

    if (value instanceof Double) {
      return PINOT_DOUBLE_GAUGE_MAP.computeIfAbsent(metricName, n -> {
        OpenTelemetryDoubleGauge<Double> otelDoubleGauge = SharedOtelMetricRegistry
            .getOrCreateOtelDoubleGauge(metricName, v -> (double) value);
        if (!LISTENERS.isEmpty()) {
          for (OpenTelemetryMetricsRegistryListener listener : LISTENERS) {
            listener.onMetricAdded(metricName, otelDoubleGauge);
          }
        }
        return otelDoubleGauge;
      });
    }
    // this should never happen as Pinot core only creates Gauges with Long or Double values
    throw new IllegalArgumentException(String.format("Value type %s is not supported for gauge",
        value.getClass().getSimpleName()));
  }

  @Override
  public PinotMeter newMeter(PinotMetricName pinotMetricname, String eventType, TimeUnit unit) {
    OpenTelemetryMetricName metricName = new OpenTelemetryMetricName(pinotMetricname.getMetricName().toString());

    return PINOT_METER_MAP.computeIfAbsent(metricName, n -> {
      OpenTelemetryMeter otelMeter = SharedOtelMetricRegistry.getOrCreateOtelMeter(metricName, eventType, unit);
      if (!LISTENERS.isEmpty()) {
        for (OpenTelemetryMetricsRegistryListener listener : LISTENERS) {
          listener.onMetricAdded(metricName, otelMeter);
        }
      }
      return otelMeter;
    });
  }

  @Override
  public PinotCounter newCounter(PinotMetricName pinotMetricname) {
    OpenTelemetryMetricName metricName = new OpenTelemetryMetricName(pinotMetricname.getMetricName().toString());

    return PINOT_COUNTER_MAP.computeIfAbsent(metricName, n -> {
      OpenTelemetryCounter otelCounter = SharedOtelMetricRegistry.getOrCreateOtelCounter(metricName);
      if (!LISTENERS.isEmpty()) {
        for (OpenTelemetryMetricsRegistryListener listener : LISTENERS) {
          listener.onMetricAdded(metricName, otelCounter);
        }
      }
      return otelCounter;
    });
  }

  /**
   * Creates a new timer. A timer's duration is the total amount of time it is set to run, while its rate is the
   * frequency at which it performs its task or counts its ticks.
   */
  @Override
  public PinotTimer newTimer(PinotMetricName pinotMetricname, TimeUnit durationUnit, TimeUnit rateUnit) {
    OpenTelemetryMetricName metricName = new OpenTelemetryMetricName(pinotMetricname.getMetricName().toString());

    return PINOT_TIMER_MAP.computeIfAbsent(metricName, n -> {
      OpenTelemetryTimer otelTimer = SharedOtelMetricRegistry.getOrCreateOtelTimer(metricName, rateUnit);
      if (!LISTENERS.isEmpty()) {
        for (OpenTelemetryMetricsRegistryListener listener : LISTENERS) {
          listener.onMetricAdded(metricName, otelTimer);
        }
      }
      return otelTimer;
    });
  }

  @Override
  public PinotHistogram newHistogram(PinotMetricName pinotMetricname, boolean biased) {
    OpenTelemetryMetricName metricName = new OpenTelemetryMetricName(pinotMetricname.getMetricName().toString());

    return PINOT_HISTOGRAM_MAP.computeIfAbsent(metricName, n -> {
      OpenTelemetryHistogram otelHistogram = SharedOtelMetricRegistry.getOrCreateOtelHistogram(metricName);
      if (!LISTENERS.isEmpty()) {
        for (OpenTelemetryMetricsRegistryListener listener : LISTENERS) {
          listener.onMetricAdded(metricName, otelHistogram);
        }
      }
      return otelHistogram;
    });
  }

  @Override
  public Map<PinotMetricName, PinotMetric> allMetrics() {
    Map<PinotMetricName, PinotMetric> allMetrics = new HashMap<>();
    allMetrics.putAll(PINOT_COUNTER_MAP);
    allMetrics.putAll(PINOT_METER_MAP);
    allMetrics.putAll(PINOT_LONG_GAUGE_MAP);
    allMetrics.putAll(PINOT_DOUBLE_GAUGE_MAP);
    allMetrics.putAll(PINOT_HISTOGRAM_MAP);
    allMetrics.putAll(PINOT_TIMER_MAP);
    return allMetrics;
  }

  @Override
  public void addListener(PinotMetricsRegistryListener listener) {
    if (listener != null) {
      LISTENERS.add((OpenTelemetryMetricsRegistryListener) listener.getMetricsRegistryListener());
    }
  }

  @Override
  public Object getMetricsRegistry() {
    // This method is here because some metric libraries (e.g. Dropwizard and Yammer) have built-in metrics registry
    // and its reporter needs to access the metrics registry instance to get all the metrics. However, OpenTelemetry
    // does not have a concept of metrics registry but a MetricsProvider (or MeterProvider in OpenTelemetry
    // terminology) instead. In addition, OpenTelemetry HTTP reporter/exporter accesses the global static
    // MetricsProvider, so we can return anything here.
    return ImmutableMap.of(
        "counters", PINOT_COUNTER_MAP,
        "meters", PINOT_METER_MAP,
        "longGauges", PINOT_LONG_GAUGE_MAP,
        "doubleGauges", PINOT_DOUBLE_GAUGE_MAP,
        "histograms", PINOT_HISTOGRAM_MAP,
        "timers", PINOT_TIMER_MAP
    );
  }

  @Override
  public void shutdown() {
    PINOT_COUNTER_MAP.clear();
    PINOT_METER_MAP.clear();
    PINOT_LONG_GAUGE_MAP.clear();
    PINOT_DOUBLE_GAUGE_MAP.clear();
    PINOT_HISTOGRAM_MAP.clear();
    PINOT_TIMER_MAP.clear();

    SharedOtelMetricRegistry.clear();

    LISTENERS.clear();
  }

  // SharedOtelMetricRegistry maintains a map of OpenTelemetry metrics that can be reused by multiple Pinot metrics.
  // For example, two Pinot metrics with the same metric name but different tableName can share the same underlying
  // OpenTelemetry metric where tableName will be a dimension/attribute. This avoids creating too many OpenTelemetry
  // metric instances.
  static class SharedOtelMetricRegistry {
    private static final Map<String, OpenTelemetryCounter> OTEL_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final Map<String, OpenTelemetryMeter> OTEL_METER_MAP = new ConcurrentHashMap<>();
    private static final Map<String, OpenTelemetryLongGauge<Long>> OTEL_LONG_GAUGE_MAP = new ConcurrentHashMap<>();
    private static final Map<String, OpenTelemetryDoubleGauge<Double>> OTEL_DOUBLE_GAUGE_MAP =
        new ConcurrentHashMap<>();
    private static final Map<String, OpenTelemetryHistogram> OTEL_HISTOGRAM_MAP = new ConcurrentHashMap<>();
    private static final Map<String, OpenTelemetryTimer> OTEL_TIMER_MAP = new ConcurrentHashMap<>();

    public static OpenTelemetryCounter getOrCreateOtelCounter(OpenTelemetryMetricName metricName) {
      return OTEL_COUNTER_MAP.computeIfAbsent(metricName.getMetricName(), n -> new OpenTelemetryCounter(metricName));
    }

    public static OpenTelemetryMeter getOrCreateOtelMeter(OpenTelemetryMetricName metricName, String eventType,
        TimeUnit unit) {
      return OTEL_METER_MAP.computeIfAbsent(metricName.getMetricName(),
          n -> new OpenTelemetryMeter(metricName, eventType, unit));
    }

    public static OpenTelemetryLongGauge<Long> getOrCreateOtelLongGauge(OpenTelemetryMetricName metricName,
        Function<Void, Long> valueSupplier) {
      return OTEL_LONG_GAUGE_MAP.computeIfAbsent(metricName.getMetricName(),
          n -> new OpenTelemetryLongGauge<>(metricName, valueSupplier));
    }

    public static OpenTelemetryDoubleGauge<Double> getOrCreateOtelDoubleGauge(OpenTelemetryMetricName metricName,
        Function<Void, Double> valueSupplier) {
      return OTEL_DOUBLE_GAUGE_MAP.computeIfAbsent(metricName.getMetricName(),
          n -> new OpenTelemetryDoubleGauge<>(metricName, valueSupplier));
    }

    public static OpenTelemetryHistogram getOrCreateOtelHistogram(OpenTelemetryMetricName metricName) {
      return OTEL_HISTOGRAM_MAP.computeIfAbsent(metricName.getMetricName(),
          n -> new OpenTelemetryHistogram(metricName));
    }

    public static OpenTelemetryTimer getOrCreateOtelTimer(OpenTelemetryMetricName metricName, TimeUnit rateUnit) {
      return OTEL_TIMER_MAP.computeIfAbsent(metricName.getMetricName(),
          n -> new OpenTelemetryTimer(metricName, rateUnit));
    }

    public static void clear() {
      OTEL_COUNTER_MAP.clear();
      OTEL_METER_MAP.clear();
      OTEL_LONG_GAUGE_MAP.clear();
      OTEL_DOUBLE_GAUGE_MAP.clear();
      OTEL_HISTOGRAM_MAP.clear();
      OTEL_TIMER_MAP.clear();
    }
  }
}

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
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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


public class OpenTelemetryMetricsRegistry implements PinotMetricsRegistry {
  private static final Map<PinotMetricName, OpenTelemetryCounter> COUNTER_MAP = new ConcurrentHashMap<>();
  private static final Map<PinotMetricName, OpenTelemetryMeter> METER_MAP = new ConcurrentHashMap<>();
  private static final Map<PinotMetricName, OpenTelemetryGauge> GAUGE_MAP = new ConcurrentHashMap<>();
  private static final Map<PinotMetricName, OpenTelemetryHistogram> HISTOGRAM_MAP = new ConcurrentHashMap<>();
  private static final Map<PinotMetricName, OpenTelemetryTimer> TIMER_MAP = new ConcurrentHashMap<>();
  private static final List<OpenTelemetryMetricsRegistryListener> LISTENERS = new LinkedList<>();

  public static final Meter OTEL_METER_PROVIDER = GlobalOpenTelemetry
      .getMeterProvider()
      .meterBuilder("pinot")
      .build();

  public OpenTelemetryMetricsRegistry() {
  }

  @Override
  public void removeMetric(PinotMetricName name) {
    COUNTER_MAP.remove(name);
    METER_MAP.remove(name);
    GAUGE_MAP.remove(name);
    HISTOGRAM_MAP.remove(name);
    TIMER_MAP.remove(name);

    if (!LISTENERS.isEmpty()) {
      OpenTelemetryMetricName otelName = new OpenTelemetryMetricName(name.getMetricName().toString());
      for (OpenTelemetryMetricsRegistryListener listener : LISTENERS) {
        listener.onMetricRemoved(otelName);
      }
    }
  }

  @Override
  public <T> PinotGauge<T> newGauge(PinotMetricName name, PinotGauge<T> gauge) {
    OpenTelemetryMetricName otelName = new OpenTelemetryMetricName(name.getMetricName().toString());
    return GAUGE_MAP.computeIfAbsent(otelName, n -> {
      OpenTelemetryGauge<T> newOtelGauge = (OpenTelemetryGauge) gauge.getGauge();

      if (!LISTENERS.isEmpty()) {
        for (OpenTelemetryMetricsRegistryListener listener : LISTENERS) {
          listener.onMetricAdded(otelName, newOtelGauge);
        }
      }
      return newOtelGauge;
    });
  }

  @Override
  public PinotMeter newMeter(PinotMetricName name, String eventType, TimeUnit unit) {
    OpenTelemetryMetricName otelName = new OpenTelemetryMetricName(name.getMetricName().toString());
    return METER_MAP.computeIfAbsent(otelName, n -> {
      OpenTelemetryMeter otelMeter = new OpenTelemetryMeter(otelName, eventType, unit);
      if (!LISTENERS.isEmpty()) {
        for (OpenTelemetryMetricsRegistryListener listener : LISTENERS) {
          listener.onMetricAdded(otelName, otelMeter);
        }
      }
      return otelMeter;
    });
  }

  @Override
  public PinotCounter newCounter(PinotMetricName name) {
    OpenTelemetryMetricName otelName = new OpenTelemetryMetricName(name.getMetricName().toString());
    return COUNTER_MAP.computeIfAbsent(otelName, n -> {
      OpenTelemetryCounter otelCounter = new OpenTelemetryCounter(otelName);
      if (!LISTENERS.isEmpty()) {
        for (OpenTelemetryMetricsRegistryListener listener : LISTENERS) {
          listener.onMetricAdded(otelName, otelCounter);
        }
      }
      return otelCounter;
    });
  }

  @Override
  public PinotTimer newTimer(PinotMetricName name, TimeUnit durationUnit, TimeUnit rateUnit) {
    OpenTelemetryMetricName otelName = new OpenTelemetryMetricName(name.getMetricName().toString());
    return TIMER_MAP.computeIfAbsent(otelName, n -> {
      OpenTelemetryTimer otelTimer = new OpenTelemetryTimer(otelName);
      if (!LISTENERS.isEmpty()) {
        for (OpenTelemetryMetricsRegistryListener listener : LISTENERS) {
          listener.onMetricAdded(otelName, otelTimer);
        }
      }
      return otelTimer;
    });
  }

  @Override
  public PinotHistogram newHistogram(PinotMetricName name, boolean biased) {
    OpenTelemetryMetricName otelName = new OpenTelemetryMetricName(name.getMetricName().toString());
    return HISTOGRAM_MAP.computeIfAbsent(otelName, n -> {
      OpenTelemetryHistogram otelHistogram = new OpenTelemetryHistogram(otelName);
      if (!LISTENERS.isEmpty()) {
        for (OpenTelemetryMetricsRegistryListener listener : LISTENERS) {
          listener.onMetricAdded(otelName, otelHistogram);
        }
      }
      return otelHistogram;
    });
  }

  @Override
  public Map<PinotMetricName, PinotMetric> allMetrics() {
    Map<PinotMetricName, PinotMetric> allMetrics = new HashMap<>();
    for (Map.Entry<PinotMetricName, OpenTelemetryCounter> entry : COUNTER_MAP.entrySet()) {
      allMetrics.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<PinotMetricName, OpenTelemetryMeter> entry : METER_MAP.entrySet()) {
      allMetrics.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<PinotMetricName, OpenTelemetryGauge> entry : GAUGE_MAP.entrySet()) {
      allMetrics.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<PinotMetricName, OpenTelemetryHistogram> entry : HISTOGRAM_MAP.entrySet()) {
      allMetrics.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<PinotMetricName, OpenTelemetryTimer> entry : TIMER_MAP.entrySet()) {
      allMetrics.put(entry.getKey(), entry.getValue());
    }
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
    return ImmutableMap.of(
        "counters", COUNTER_MAP,
        "meters", METER_MAP,
        "gauges", GAUGE_MAP,
        "histograms", HISTOGRAM_MAP,
        "timers", TIMER_MAP);
  }

  @Override
  public void shutdown() {
    COUNTER_MAP.clear();
    METER_MAP.clear();
    GAUGE_MAP.clear();
    HISTOGRAM_MAP.clear();
    TIMER_MAP.clear();
    LISTENERS.clear();
  }
}

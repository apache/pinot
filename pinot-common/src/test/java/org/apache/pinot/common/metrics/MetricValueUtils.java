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
package org.apache.pinot.common.metrics;

import org.apache.pinot.spi.metrics.PinotMetric;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.SettableValue;


/**
 * Test utility for reading gauge values from {@link AbstractMetrics} without depending on a specific metrics
 * implementation. Reads via {@link AbstractMetrics#getGaugeValue(String)} first (covers gauges registered via the
 * {@code setValueOf*Gauge}/{@code addValueTo*Gauge} family that populate the internal value map), and falls back to
 * looking the metric up in the registry and reading it via the {@link SettableValue} SPI (covers pure-supplier paths
 * like {@code setOrUpdateGauge(Supplier)}, {@code setOrUpdateGauge(long)}, and {@code addCallbackGauge}).
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class MetricValueUtils {
  private MetricValueUtils() {
  }

  public static boolean gaugeExists(AbstractMetrics metrics, String metricName) {
    return readGauge(metrics, metricName) != null;
  }

  public static long getGaugeValue(AbstractMetrics metrics, String metricName) {
    Long value = readGauge(metrics, metricName);
    return value != null ? value : 0L;
  }

  public static boolean globalGaugeExists(AbstractMetrics metrics, AbstractMetrics.Gauge gauge) {
    return readGauge(metrics, gauge.getGaugeName()) != null;
  }

  public static long getGlobalGaugeValue(AbstractMetrics metrics, AbstractMetrics.Gauge gauge) {
    Long value = readGauge(metrics, gauge.getGaugeName());
    return value != null ? value : 0L;
  }

  public static boolean globalGaugeExists(AbstractMetrics metrics, String key, AbstractMetrics.Gauge gauge) {
    return readGauge(metrics, gauge.getGaugeName() + "." + key) != null;
  }

  public static long getGlobalGaugeValue(AbstractMetrics metrics, String key, AbstractMetrics.Gauge gauge) {
    Long value = readGauge(metrics, gauge.getGaugeName() + "." + key);
    return value != null ? value : 0L;
  }

  public static boolean tableGaugeExists(AbstractMetrics metrics, String tableName, AbstractMetrics.Gauge gauge) {
    return readGauge(metrics, gauge.getGaugeName() + "." + tableName) != null;
  }

  public static long getTableGaugeValue(AbstractMetrics metrics, String tableName, AbstractMetrics.Gauge gauge) {
    Long value = readGauge(metrics, gauge.getGaugeName() + "." + tableName);
    return value != null ? value : 0L;
  }

  public static boolean tableGaugeExists(AbstractMetrics metrics, String tableName, String key,
      AbstractMetrics.Gauge gauge) {
    return readGauge(metrics, gauge.getGaugeName() + "." + tableName + "." + key) != null;
  }

  public static long getTableGaugeValue(AbstractMetrics metrics, String tableName, String key,
      AbstractMetrics.Gauge gauge) {
    Long value = readGauge(metrics, gauge.getGaugeName() + "." + tableName + "." + key);
    return value != null ? value : 0L;
  }

  public static boolean partitionGaugeExists(AbstractMetrics metrics, String tableName, int partitionId,
      AbstractMetrics.Gauge gauge) {
    return readGauge(metrics, gauge.getGaugeName() + "." + tableName + "." + partitionId) != null;
  }

  public static long getPartitionGaugeValue(AbstractMetrics metrics, String tableName, int partitionId,
      AbstractMetrics.Gauge gauge) {
    Long value = readGauge(metrics, gauge.getGaugeName() + "." + tableName + "." + partitionId);
    return value != null ? value : 0L;
  }

  private static Long readGauge(AbstractMetrics metrics, String metricName) {
    // Fast path: gauges registered via setValueOf*/addValueTo* populate _gaugeValues directly.
    Long direct = metrics.getGaugeValue(metricName);
    if (direct != null) {
      return direct;
    }
    // Fallback: gauges registered via pure-supplier paths (setOrUpdateGauge(Supplier), setOrUpdateGauge(long),
    // addCallbackGauge) are only in the registry. Look them up and read via the SettableValue SPI, which is
    // implemented by YammerSettableGauge and DropwizardSettableGauge.
    PinotMetricName name =
        PinotMetricUtils.makePinotMetricName(metrics.getClass(), metrics.getMetricPrefix() + metricName);
    PinotMetric pinotMetric = metrics.getMetricsRegistry().allMetrics().get(name);
    if (pinotMetric == null) {
      return null;
    }
    Object inner = pinotMetric.getMetric();
    if (!(inner instanceof SettableValue)) {
      return null;
    }
    Object value = ((SettableValue<?>) inner).getValue();
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    return null;
  }
}

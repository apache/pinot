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

import com.yammer.metrics.core.MetricName;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricName;
import org.apache.pinot.plugin.metrics.yammer.YammerSettableGauge;
import org.apache.pinot.spi.metrics.PinotMetric;


public class MetricValueUtils {
  private MetricValueUtils() {
  }

  public static boolean gaugeExists(AbstractMetrics metrics, String metricName) {
    return extractMetric(metrics, metricName) != null;
  }

  public static long getGaugeValue(AbstractMetrics metrics, String metricName) {
    PinotMetric pinotMetric = extractMetric(metrics, metricName);
    if (pinotMetric == null) {
      return 0;
    }
    return ((YammerSettableGauge<Long>) pinotMetric.getMetric()).value();
  }

  public static boolean globalGaugeExists(AbstractMetrics metrics, AbstractMetrics.Gauge gauge) {
    return extractMetric(metrics, gauge.getGaugeName()) != null;
  }

  public static long getGlobalGaugeValue(AbstractMetrics metrics, AbstractMetrics.Gauge gauge) {
    PinotMetric pinotMetric = extractMetric(metrics, gauge.getGaugeName());
    if (pinotMetric == null) {
      return 0;
    }
    return ((YammerSettableGauge<Long>) pinotMetric.getMetric()).value();
  }

  public static boolean globalGaugeExists(AbstractMetrics metrics, String key, AbstractMetrics.Gauge gauge) {
    return extractMetric(metrics, gauge.getGaugeName() + "." + key) != null;
  }

  public static long getGlobalGaugeValue(AbstractMetrics metrics, String key, AbstractMetrics.Gauge gauge) {
    PinotMetric pinotMetric = extractMetric(metrics, gauge.getGaugeName() + "." + key);
    if (pinotMetric == null) {
      return 0;
    }
    return ((YammerSettableGauge<Long>) pinotMetric.getMetric()).value();
  }

  public static boolean tableGaugeExists(AbstractMetrics metrics, String tableName, AbstractMetrics.Gauge gauge) {
    return extractMetric(metrics, gauge.getGaugeName() + "." + tableName) != null;
  }

  public static long getTableGaugeValue(AbstractMetrics metrics, String tableName, AbstractMetrics.Gauge gauge) {
    PinotMetric pinotMetric = extractMetric(metrics, gauge.getGaugeName() + "." + tableName);
    if (pinotMetric == null) {
      return 0;
    }
    return ((YammerSettableGauge<Long>) pinotMetric.getMetric()).value();
  }

  public static boolean tableGaugeExists(AbstractMetrics metrics, String tableName, String key,
      AbstractMetrics.Gauge gauge) {
    return extractMetric(metrics, gauge.getGaugeName() + "." + tableName + "." + key) != null;
  }

  public static long getTableGaugeValue(AbstractMetrics metrics, String tableName, String key,
      AbstractMetrics.Gauge gauge) {
    PinotMetric pinotMetric = extractMetric(metrics, gauge.getGaugeName() + "." + tableName + "." + key);
    if (pinotMetric == null) {
      return 0;
    }
    return ((YammerSettableGauge<Long>) pinotMetric.getMetric()).value();
  }

  public static boolean partitionGaugeExists(AbstractMetrics metrics, String tableName, int partitionId,
      AbstractMetrics.Gauge gauge) {
    return extractMetric(metrics, gauge.getGaugeName() + "." + tableName + "." + partitionId) != null;
  }

  public static long getPartitionGaugeValue(AbstractMetrics metrics, String tableName, int partitionId,
      AbstractMetrics.Gauge gauge) {
    PinotMetric pinotMetric = extractMetric(metrics, gauge.getGaugeName() + "." + tableName + "." + partitionId);
    if (pinotMetric == null) {
      return 0;
    }
    return ((YammerSettableGauge<Long>) pinotMetric.getMetric()).value();
  }

  private static PinotMetric extractMetric(AbstractMetrics metrics, String metricName) {
    String metricPrefix;
    if (metrics instanceof ControllerMetrics) {
      metricPrefix = "pinot.controller.";
    } else if (metrics instanceof BrokerMetrics) {
      metricPrefix = "pinot.broker.";
    } else if (metrics instanceof ServerMetrics) {
      metricPrefix = "pinot.server.";
    } else if (metrics instanceof MinionMetrics) {
      metricPrefix = "pinot.minion.";
    } else {
      throw new RuntimeException("unsupported AbstractMetrics type: " + metrics.getClass().toString());
    }
    return metrics.getMetricsRegistry().allMetrics()
        .get(new YammerMetricName(new MetricName(metrics.getClass(), metricPrefix + metricName)));
  }
}

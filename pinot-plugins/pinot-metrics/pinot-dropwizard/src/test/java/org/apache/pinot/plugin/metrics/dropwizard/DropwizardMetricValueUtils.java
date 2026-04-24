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

import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.metrics.PinotMetric;


/**
 * Dropwizard-specific test utility for reading gauge values directly from the Dropwizard metrics registry. Mirrors the
 * Yammer-side {@code YammerMetricValueUtils}.
 */
public class DropwizardMetricValueUtils {
  private DropwizardMetricValueUtils() {
  }

  public static long getGaugeValue(AbstractMetrics<?, ?, ?, ?> metrics, String metricName) {
    PinotMetric pinotMetric = extractMetric(metrics, metricName);
    if (pinotMetric == null) {
      return 0;
    }
    Object gauge = pinotMetric.getMetric();
    if (!(gauge instanceof DropwizardSettableGauge)) {
      throw new IllegalStateException("Expected DropwizardSettableGauge for " + metricName + " but got: " + gauge);
    }
    Object value = ((DropwizardSettableGauge<?>) gauge).getValue();
    if (!(value instanceof Number)) {
      throw new IllegalStateException("Gauge did not produce a Number: " + metricName + " -> " + value);
    }
    return ((Number) value).longValue();
  }

  private static PinotMetric extractMetric(AbstractMetrics<?, ?, ?, ?> metrics, String metricName) {
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
      throw new IllegalArgumentException("Unsupported AbstractMetrics type: " + metrics.getClass());
    }
    return metrics.getMetricsRegistry().allMetrics().get(new DropwizardMetricName(metricPrefix + metricName));
  }
}

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

import com.google.auto.service.AutoService;
import java.util.function.Function;
import org.apache.pinot.spi.annotations.metrics.MetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotJmxReporter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricReporter;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;


@AutoService(PinotMetricsFactory.class)
@MetricsFactory
public class OpenTelemetryMetricsFactory implements PinotMetricsFactory {
  private final PinotMetricsRegistry _pinotMetricsRegistry = new OpenTelemetryMetricsRegistry();

  @Override
  public void init(PinotConfiguration metricsConfiguration) {
  }

  @Override
  public PinotMetricsRegistry getPinotMetricsRegistry() {
    return _pinotMetricsRegistry;
  }

  @Override
  public PinotMetricName makePinotMetricName(Class<?> klass, String name) {
    // It's OK to just throw away the class name. Adding the class name to into pinot metric name will make
    // the dimension/attribute parsing logic more complicated as not all pinot metric having it.
    return new OpenTelemetryMetricName(name);
  }

  @Override
  @Deprecated
  public <T> PinotGauge<T> makePinotGauge(Function<Void, T> condition) {
    return makePinotGauge("UnknownPinotMetricName", condition);
  }

  @Override
  public <T> PinotGauge<T> makePinotGauge(String metricName, Function<Void, T> condition) {
    T value = condition.apply(null);
    if (value instanceof Integer || value instanceof Long) {
      return new OpenTelemetryLongGauge<>(new OpenTelemetryMetricName(metricName), condition);
    }
    if (value instanceof Double) {
      return new OpenTelemetryDoubleGauge<>(new OpenTelemetryMetricName(metricName), condition);
    }
    throw new IllegalArgumentException("Unsupported supplier type: " + condition.getClass().getName());
  }

  @Override
  @Deprecated
  public PinotJmxReporter makePinotJmxReporter(PinotMetricsRegistry metricsRegistry) {
    throw new UnsupportedOperationException("OpenTelemetry does not support JMX reporter");
  }

  @Override
  public PinotMetricReporter makePinotMetricReporter(PinotMetricsRegistry metricsRegistry) {
    return new OpenTelemetryHttpReporter();
  }

  @Override
  public String getMetricsFactoryName() {
    return "OpenTelemetry";
  }
}

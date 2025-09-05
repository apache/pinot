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
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleGauge;
import io.opentelemetry.api.metrics.LongGauge;
import java.util.Map;
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
  @Deprecated
  public PinotMetricName makePinotMetricName(Class<?> klass, String name) {
    throw new UnsupportedOperationException("Please use makePinotMetricName(Class, String, String, Map) instead");
  }

  @Override
  public PinotMetricName makePinotMetricName(Class<?> klass, String fullName, String simplifiedName,
      Map<String, String> attributes) {
    return new OpenTelemetryMetricName(fullName, simplifiedName, attributes);
  }

  @Override
  @Deprecated
  public <T> PinotGauge<T> makePinotGauge(Function<Void, T> condition) {
    throw new UnsupportedOperationException("Please use makePinotGauge(PinotMetricName, Function) instead");
  }

  @Override
  @Deprecated
  public <T> PinotGauge<T> makePinotGauge(String metricName, Function<Void, T> condition) {
    throw new UnsupportedOperationException("Please use makePinotGauge(PinotMetricName, Function) instead");
  }

  @Override
  public <T> PinotGauge<T> makePinotGauge(PinotMetricName pinotMetricName, Function<Void, T> condition) {
    T value = condition.apply(null);

    String simplifiedMetricName = pinotMetricName.getSimplifiedMetricName();
    Attributes attributes = OpenTelemetryUtil.toOpenTelemetryAttributes(pinotMetricName.getAttributes());

    if (value instanceof Integer || value instanceof Long) {
      LongGauge longGauge = OpenTelemetryMetricsRegistry.SharedOtelMetricRegistry
          .getOrCreateLongGauge(simplifiedMetricName);
      return new OpenTelemetryLongGauge<>(longGauge, attributes, condition);
    }
    if (value instanceof Double) {
      DoubleGauge doubleGauge = OpenTelemetryMetricsRegistry.SharedOtelMetricRegistry
          .getOrCreateDoubleGauge(simplifiedMetricName);
      return new OpenTelemetryDoubleGauge<>(doubleGauge, attributes, condition);
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

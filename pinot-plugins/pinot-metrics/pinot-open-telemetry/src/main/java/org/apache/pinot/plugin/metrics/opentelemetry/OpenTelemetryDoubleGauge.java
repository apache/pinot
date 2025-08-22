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

import io.opentelemetry.api.metrics.DoubleGauge;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pinot.spi.metrics.PinotGauge;


/**
 * OpenTelemetryDoubleGauge is the implementation of {@link PinotGauge} for OpenTelemetry.
 * Most of metric libraries including OpenTelemetry do not support retrieve the current value of a gauge, so it allows
 * setting a value supplier to get the current value when calling {@link #value()}.
 */
public class OpenTelemetryDoubleGauge<T> implements PinotGauge<T> {
  private final OpenTelemetryMetricName _metricName;
  private final DoubleGauge _doubleGauge;
  private Supplier<T> _valueSupplier;

  public OpenTelemetryDoubleGauge(OpenTelemetryMetricName metricName, Function<Void, T> valueSupplier) {
    _doubleGauge = OpenTelemetryMetricsRegistry.getOtelMeterProvider()
        .gaugeBuilder(metricName.getOtelMetricName())
        .build();
    _metricName = metricName;
    setValueSupplier(() -> valueSupplier.apply(null));
  }

  @Override
  public Object getGauge() {
    return _doubleGauge;
  }

  @Override
  public Object getMetric() {
    return _doubleGauge;
  }

  @Override
  public T value() {
    return _valueSupplier.get();
  }

  @Override
  public void setValue(T value) {
    // records the metric with a value with a set of attributes parsed from Pinot metric name
    _doubleGauge.set((Double) value, _metricName.getOtelAttributes());
    _valueSupplier = () -> value;
  }

  @Override
  public void setValueSupplier(Supplier<T> valueSupplier) {
    _valueSupplier = valueSupplier;
  }
}

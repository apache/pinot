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
import io.opentelemetry.api.metrics.LongGauge;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.pinot.spi.metrics.PinotGauge;


public class OpenTelemetryGauge<T> implements PinotGauge<T> {
  private OpenTelemetryMetricName _metricName;
  private LongGauge _longGauge;
  private DoubleGauge _doubleGauge;
  private Supplier<T> _valueSupplier;

  public OpenTelemetryGauge(OpenTelemetryMetricName metricName, Function<Void, T> valueSupplier) {
    if (valueSupplier instanceof LongSupplier) {
      _longGauge = OpenTelemetryMetricsRegistry.OTEL_METER_PROVIDER
          .gaugeBuilder(metricName.getOtelMetricName())
          .ofLongs()
          .build();
    } else if (valueSupplier instanceof DoubleSupplier) {
      _doubleGauge = OpenTelemetryMetricsRegistry.OTEL_METER_PROVIDER
          .gaugeBuilder(metricName.getOtelMetricName())
          .build();
    } else {
      throw new IllegalArgumentException("Unsupported value supplier type: " + _valueSupplier.getClass());
    }
    _metricName = metricName;
    setValueSupplier(() -> valueSupplier.apply(null));
  }

  @Override
  public Object getGauge() {
    return _longGauge != null ? _longGauge : _doubleGauge;
  }

  @Override
  public Object getMetric() {
    return _longGauge != null ? _longGauge : _doubleGauge;
  }

  @Override
  public T value() {
    return _valueSupplier.get();
  }

  @Override
  public void setValue(T value) {
    if (_longGauge != null && value instanceof Long) {
      _longGauge.set((Long) value, _metricName.getOtelAttributes());
    } else if (_doubleGauge != null && value instanceof Double) {
      _doubleGauge.set((Double) value, _metricName.getOtelAttributes());
    } else {
      throw new IllegalArgumentException(
          String.format("Value type %s does not match gauge type %s", value.getClass().getSimpleName(),
              _longGauge != null ? "LongGauge" : "DoubleGauge")
      );
    }
    _valueSupplier = () -> value;
  }

  @Override
  public void setValueSupplier(Supplier<T> valueSupplier) {
    _valueSupplier = valueSupplier;
  }
}

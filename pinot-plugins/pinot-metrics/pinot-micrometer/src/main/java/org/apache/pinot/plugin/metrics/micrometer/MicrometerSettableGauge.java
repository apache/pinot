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

import java.util.function.Supplier;
import org.apache.pinot.spi.metrics.SettableValue;


/// Mutable value holder behind a native Micrometer {@code Gauge}.
///
/// <p>A Micrometer {@code Gauge} samples a function over a fixed reference object at scrape time. We register the
/// gauge over an instance of this holder (see {@code MicrometerMetricsRegistry#newGauge}) and let application code
/// rewrite the underlying value or value supplier through {@link #setValue}/{@link #setValueSupplier}. The gauge's
/// sampling function then reads the latest value via {@link #getValue}.
///
/// <p>Thread-safety: {@code _valueSupplier} is declared {@code volatile} so that writes from application threads
/// (via {@link #setValue} or {@link #setValueSupplier}) are visible to Micrometer's scrape thread (via
/// {@link #getValue}).
///
/// @param <T> the type of the metric's value
public class MicrometerSettableGauge<T> implements SettableValue<T> {
  private volatile Supplier<T> _valueSupplier;

  public MicrometerSettableGauge(Supplier<T> valueSupplier) {
    setValueSupplier(valueSupplier);
  }

  public MicrometerSettableGauge(T value) {
    setValue(value);
  }

  @Override
  public void setValueSupplier(Supplier<T> valueSupplier) {
    _valueSupplier = valueSupplier;
  }

  @Override
  public void setValue(T value) {
    _valueSupplier = () -> value;
  }

  @Override
  public T getValue() {
    return _valueSupplier.get();
  }
}

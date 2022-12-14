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

import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pinot.spi.metrics.PinotGauge;

public class DropwizardGauge<T> implements PinotGauge<T> {

  private final DropwizardSettableGauge<T> _settableGauge;

  public DropwizardGauge(DropwizardSettableGauge<T> settableGauge) {
    _settableGauge = settableGauge;
  }

  public DropwizardGauge(Function<Void, T> condition) {
    this(new DropwizardSettableGauge<>(() -> condition.apply(null)));
  }

  @Override
  public Object getGauge() {
    return _settableGauge;
  }

  @Override
  public Object getMetric() {
    return _settableGauge;
  }

  @Override
  public T value() {
    return _settableGauge.getValue();
  }

  @Override
  public void setValue(T value) {
    _settableGauge.setValue(value);
  }

  @Override
  public void setValueSupplier(Supplier<T> valueSupplier) {
    _settableGauge.setValueSupplier(valueSupplier);
  }
}

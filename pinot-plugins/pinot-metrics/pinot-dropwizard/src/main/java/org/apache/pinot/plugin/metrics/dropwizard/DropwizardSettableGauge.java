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

import com.codahale.metrics.SettableGauge;
import java.util.function.Supplier;
import org.apache.pinot.spi.metrics.SettableValue;

/**
 * DropwizardSettableGauge extends {@link SettableGauge} and implements {@link SettableValue}, allowing setting a value
 * or a value supplier to provide the gauge value
 *
 * @param <T> the type of the metric's value
 */
public class DropwizardSettableGauge<T> implements SettableGauge<T>, SettableValue<T> {
  private Supplier<T> _valueSupplier;

  public DropwizardSettableGauge(Supplier<T> valueSupplier) {
    setValueSupplier(valueSupplier);
  }

  public DropwizardSettableGauge(T value) {
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

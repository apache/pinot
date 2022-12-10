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
package org.apache.pinot.plugin.metrics.yammer;

import java.util.function.Supplier;

public class YammerSettableGaugeImpl<T> extends YammerSettableGauge<T> {
  private Supplier<T> _valueSupplier;

  public YammerSettableGaugeImpl(Supplier<T> valueSupplier) {
    _valueSupplier = valueSupplier;
  }

  @Override
  void setValue(T value) {
    _valueSupplier = () -> value;
  }

  @Override
  void setValueSupplier(Supplier<T> valueSupplier) {
    _valueSupplier = valueSupplier;
  }

  @Override
  public T value() {
    return _valueSupplier.get();
  }
}

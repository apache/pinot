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
package org.apache.pinot.plugin.metrics.compound;

import java.util.List;
import java.util.function.Supplier;
import org.apache.pinot.spi.metrics.PinotGauge;


public class CompoundPinotGauge<T> extends AbstractCompoundPinotMetric<PinotGauge<T>> implements PinotGauge<T> {
  public CompoundPinotGauge(List<PinotGauge<T>> meters) {
    super(meters);
  }

  @Override
  public Object getGauge() {
    return getSomeMeter().getGauge();
  }

  @Override
  public T value() {
    return getSomeMeter().value();
  }

  @Override
  public void setValue(T value) {
    _metrics.forEach(m -> m.setValue(value));
  }

  @Override
  public void setValueSupplier(Supplier<T> valueSupplier) {
    _metrics.forEach(m -> m.setValueSupplier(valueSupplier));
  }
}

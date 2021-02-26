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
package org.apache.pinot.common.metrics.yammer;

import com.yammer.metrics.core.Gauge;
import java.util.function.Function;
import org.apache.pinot.spi.metrics.PinotGauge;


public class YammerGauge<T> implements PinotGauge<T> {

  private final Gauge<T> _gauge;

  public YammerGauge(Gauge<T> gauge) {
    _gauge = gauge;
  }

  public YammerGauge(Function<Void, T> condition) {
    this(new Gauge<T>() {
      @Override
      public T value() {
        return condition.apply(null);
      }
    });
  }

  @Override
  public Object getGauge() {
    return _gauge;
  }

  @Override
  public Object getMetric() {
    return _gauge;
  }

  @Override
  public T value() {
    return _gauge.value();
  }
}

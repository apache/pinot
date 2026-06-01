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
package org.apache.pinot.plugin.metrics.fake;

import java.util.function.Function;
import org.apache.pinot.spi.annotations.metrics.MetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotJmxReporter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;


/**
 * A test-only {@link PinotMetricsFactory} backed by the in-memory {@link FakePinotMetricsRegistry}. Used by
 * pinot-common tests so they can exercise {@code AbstractMetrics} without depending on a plugin module.
 */
@MetricsFactory
public class FakeMetricsFactory implements PinotMetricsFactory {
  private PinotMetricsRegistry _registry;

  @Override
  public void init(PinotConfiguration metricsConfiguration) {
  }

  @Override
  public PinotMetricsRegistry getPinotMetricsRegistry() {
    if (_registry == null) {
      _registry = new FakePinotMetricsRegistry();
    }
    return _registry;
  }

  @Override
  public PinotMetricName makePinotMetricName(Class<?> clazz, String name) {
    return new FakePinotMetricName(clazz, name);
  }

  @Override
  public <T> PinotGauge<T> makePinotGauge(Function<Void, T> condition) {
    return new FakePinotGauge<>(condition);
  }

  @Override
  public PinotJmxReporter makePinotJmxReporter(PinotMetricsRegistry metricsRegistry) {
    // No JMX for the fake — intentional, to avoid cross-test collisions.
    return () -> {
    };
  }

  @Override
  public String getMetricsFactoryName() {
    return "Fake";
  }
}

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
package org.apache.pinot.spi.annotations.metrics;

import java.util.Map;
import java.util.function.Function;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.NoopPinotMetricsRegistry;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotJmxReporter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;


/**
 * Factory for generating objects of Pinot metrics.
 */
public interface PinotMetricsFactory {

  /**
   * Initializes the Pinot metrics factory.
   */
  void init(PinotConfiguration metricsConfiguration);

  /**
   * Gets {@link PinotMetricsRegistry}. There should be only one such instance in every {@link PinotMetricsRegistry}.
   */
  PinotMetricsRegistry getPinotMetricsRegistry();

  /**
   * Makes a {@link PinotMetricName} given the class and the metric name.
   */
  PinotMetricName makePinotMetricName(Class<?> klass, String name);

  /// Makes a {@link PinotMetricName} carrying both the flat name (for legacy compat) and structured dimension tags
  /// (for tag-first export). Legacy factory implementations inherit this default and ignore {@code baseName}/{@code
  /// tags}, delegating to {@link #makePinotMetricName(Class, String)} with the flat name so their output is unchanged.
  /// The native Micrometer implementation overrides this to apply tags.
  ///
  /// @param klass     the metrics class (used to derive the metric name prefix by some namers)
  /// @param flatName  the full dotted metric name including embedded table/type/partition segments (legacy compat)
  /// @param baseName  the dimension-free dotted metric name (table/type/partition stripped) used by the native backend
  /// @param tags      structured dimension tags ({@code table}, {@code tableType}, {@code partition}, …)
  default PinotMetricName makePinotMetricName(Class<?> klass, String flatName, String baseName,
      Map<String, String> tags) {
    return makePinotMetricName(klass, flatName);
  }

  /**
   * Makes a {@link PinotGauge} given a function.
   */
  <T> PinotGauge<T> makePinotGauge(Function<Void, T> condition);

  /**
   * Makes a {@link PinotJmxReporter} given a {@link PinotMetricsRegistry}.
   */
  PinotJmxReporter makePinotJmxReporter(PinotMetricsRegistry metricsRegistry);

  /**
   * Returns the name of metrics factory.
   */
  String getMetricsFactoryName();

  class Noop implements PinotMetricsFactory {
    private final NoopPinotMetricsRegistry _registry = new NoopPinotMetricsRegistry();
    @Override
    public void init(PinotConfiguration metricsConfiguration) {
    }

    @Override
    public PinotMetricsRegistry getPinotMetricsRegistry() {
      return _registry;
    }

    @Override
    public PinotMetricName makePinotMetricName(Class<?> klass, String name) {
      return () -> "noopMetricName";
    }

    @Override
    public <T> PinotGauge<T> makePinotGauge(Function<Void, T> condition) {
      return _registry.newGauge();
    }

    @Override
    public PinotJmxReporter makePinotJmxReporter(PinotMetricsRegistry metricsRegistry) {
      return () -> {
      };
    }

    @Override
    public String getMetricsFactoryName() {
      return "noop";
    }
  }
}

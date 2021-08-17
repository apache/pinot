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

import java.util.function.Function;
import org.apache.pinot.spi.env.PinotConfiguration;
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
}

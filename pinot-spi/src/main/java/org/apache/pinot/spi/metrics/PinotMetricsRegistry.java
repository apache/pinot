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
package org.apache.pinot.spi.metrics;

import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * A registry of metric instances in Pinot. This interface is to decouple from concrete metrics related classes, like
 * Yammer.
 * This interface should be thread-safe against adds and removes of the same metric.
 */
public interface PinotMetricsRegistry {

  /**
   * Removes the metric with the given name.
   *
   * @param name the name of the metric
   */
  void removeMetric(PinotMetricName name);

  /**
   * Given a new {@link PinotGauge}, registers it under the given metric name.
   *
   * @param name the name of the metric
   * @param gauge     the gauge metric
   * @param <T>        the type of the value returned by the metric
   * @return {@code metric}
   */
  <T> PinotGauge<T> newGauge(PinotMetricName name, PinotGauge<T> gauge);

  /**
   * Creates a new {@link PinotMeter} and registers it under the given metric name.
   *
   * @param name the name of the metric
   * @param eventType  the plural name of the type of events the meter is measuring (e.g., {@code
   *                   "requests"})
   * @param unit       the rate unit of the new meter
   * @return a new {@link PinotMeter}
   */
  PinotMeter newMeter(PinotMetricName name, String eventType, TimeUnit unit);

  /**
   * Creates a new {@link PinotCounter} and registers it under the given metric name.
   *
   * @param name the name of the metric
   * @return a new {@link PinotCounter}
   */
  PinotCounter newCounter(PinotMetricName name);

  /**
   * Creates a new {@link PinotTimer} and registers it under the given metric name.
   *
   * @param  name   the name of the metric
   * @param durationUnit the duration scale unit of the new timer
   * @param rateUnit     the rate scale unit of the new timer
   * @return a new {@link PinotTimer}
   */
  PinotTimer newTimer(PinotMetricName name, TimeUnit durationUnit, TimeUnit rateUnit);

  /**
   * Creates a new {@link PinotHistogram} and registers it under the given metric name.
   *
   * @param name the name of the metric
   * @param biased     whether or not the histogram should be biased
   * @return a new {@link PinotHistogram}
   */
  PinotHistogram newHistogram(PinotMetricName name, boolean biased);

  /**
   * Returns an unmodifiable map of all metrics and their names.
   *
   * @return an unmodifiable map of all metrics and their names
   */
  Map<PinotMetricName, PinotMetric> allMetrics();

  /**
   * Adds a {@link PinotMetricsRegistryListener} to a collection of listeners that will be notified on
   * metric creation.  Listeners will be notified in the order in which they are added.
   * <p/>
   * <b>N.B.:</b> The listener will be notified of all existing metrics when it first registers.
   *
   * @param listener the listener that will be notified
   */
  void addListener(PinotMetricsRegistryListener listener);

  /**
   * The the actual object of MetricsRegistry.
   */
  Object getMetricsRegistry();

  /**
   * Shut down this registry's thread pools.
   */
  void shutdown();
}

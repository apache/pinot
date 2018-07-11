/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.metrics;

import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Stoppable;


/**
 * A metrics registry which extends {@link MetricsRegistry} and provides additional
 * APIs to register aggregated metrics to registry
 *
 *
 */
public class AggregatedMetricsRegistry extends MetricsRegistry {

  /**
   * Creates a new {@link AggregatedMetricsRegistry}.
   */
  public AggregatedMetricsRegistry() {
    super();
  }

  /**
   * Creates a new {@link AggregatedMetricsRegistry} with the given {@link Clock} instance.
   *
   * @param clock    a {@link Clock} instance
   */
  public AggregatedMetricsRegistry(Clock clock) {
    super(clock);
  }

  /**
   * Creates a new {@link AggregatedCounter} and registers it under the given metric name.
   *
   * @param metricName the name of the metric
   * @return a new {@link AggregatedCounter}
   */
  public AggregatedCounter newAggregatedCounter(MetricName metricName) {
    return getOrAdd(metricName, new AggregatedCounter());
  }

  /**
   * Creates a new {@link AggregatedHistogram} and registers it under the given metric name.
   *
   * @param metricName the name of the metric
   * @return a new {@link AggregatedHistogram}
   */
  public <T extends Sampling> AggregatedHistogram<T> newAggregatedHistogram(MetricName metricName) {
    return getOrAdd(metricName, new AggregatedHistogram<T>());
  }

  /**
   * Creates a new {@link AggregatedMeter} and registers it under the given metric name.
   *
   * @param metricName the name of the metric
   * @return a new {@link AggregatedMeter}
   */
  public <T extends Metered & Stoppable> AggregatedMeter<T> newAggregatedMeter(MetricName metricName) {
    return getOrAdd(metricName, new AggregatedMeter<T>());
  }

  /**
   * Creates a new {@link AggregatedLongGauge} and registers it under the given metric name.
   *
   * @param metricName the name of the metric
   * @return a new {@link AggregatedLongGauge}
   */
  public <T extends Number, V extends Gauge<T>> AggregatedLongGauge<T, V> newAggregatedLongGauge(MetricName metricName) {
    return getOrAdd(metricName, new AggregatedLongGauge<T, V>());
  }
}

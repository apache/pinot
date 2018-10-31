/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.yammer.metrics.core.MetricsRegistry;


/**
 * Broker metrics utility class, which provides facilities to log the execution performance of queries.
 *
 */
public class BrokerMetrics extends AbstractMetrics<BrokerQueryPhase, BrokerMeter, BrokerGauge, BrokerTimer> {
  /**
   * Constructs the broker metrics.
   *
   * @param metricsRegistry The metric registry used to register timers and meters.
   */
  public BrokerMetrics(MetricsRegistry metricsRegistry) {
    super("pinot.broker.", metricsRegistry, BrokerMetrics.class);
  }

  public BrokerMetrics(MetricsRegistry metricsRegistry, boolean global) {
    super("pinot.broker.", metricsRegistry, BrokerMetrics.class, global);
  }

  @Override
  protected BrokerQueryPhase[] getQueryPhases() {
    return BrokerQueryPhase.values();
  }

  @Override
  protected BrokerMeter[] getMeters() {
    return BrokerMeter.values();
  }

  protected BrokerGauge[] getGauges() {
    return BrokerGauge.values();
  }
}

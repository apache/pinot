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
package org.apache.pinot.common.metrics;

import com.yammer.metrics.core.MetricsRegistry;

import static org.apache.pinot.common.utils.CommonConstants.Broker.DEFAULT_METRICS_GLOBAL_ENABLED;
import static org.apache.pinot.common.utils.CommonConstants.Broker.DEFAULT_METRICS_NAME_PREFIX;


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
    this(metricsRegistry, DEFAULT_METRICS_GLOBAL_ENABLED);
  }

  public BrokerMetrics(MetricsRegistry metricsRegistry, boolean global) {
    this(DEFAULT_METRICS_NAME_PREFIX, metricsRegistry, global);
  }

  public BrokerMetrics(String prefix, MetricsRegistry metricsRegistry, boolean global) {
    super(prefix, metricsRegistry, BrokerMetrics.class, global);
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

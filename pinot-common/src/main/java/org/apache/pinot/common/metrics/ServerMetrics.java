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

import static org.apache.pinot.common.utils.CommonConstants.Server.DEFAULT_METRICS_GLOBAL_ENABLED;
import static org.apache.pinot.common.utils.CommonConstants.Server.DEFAULT_METRICS_PREFIX;


/**
 * Utility class to centralize all metrics reporting for the Pinot server.
 *
 */
public class ServerMetrics extends AbstractMetrics<ServerQueryPhase, ServerMeter, ServerGauge, ServerTimer> {

  public ServerMetrics(MetricsRegistry metricsRegistry) {
    this(DEFAULT_METRICS_PREFIX, metricsRegistry, DEFAULT_METRICS_GLOBAL_ENABLED);
  }

  public ServerMetrics(MetricsRegistry metricsRegistry, boolean global) {
    this(DEFAULT_METRICS_PREFIX, metricsRegistry, global);
  }

  public ServerMetrics(String prefix, MetricsRegistry metricsRegistry, boolean global) {
    super(prefix, metricsRegistry, ServerMetrics.class, global);
  }

  @Override
  protected ServerQueryPhase[] getQueryPhases() {
    return ServerQueryPhase.values();
  }

  @Override
  protected ServerMeter[] getMeters() {
    return ServerMeter.values();
  }

  @Override
  protected ServerGauge[] getGauges() {
    return ServerGauge.values();
  }
}

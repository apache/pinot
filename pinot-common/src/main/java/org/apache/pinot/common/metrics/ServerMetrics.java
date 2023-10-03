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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;

import static org.apache.pinot.spi.utils.CommonConstants.Server.DEFAULT_ENABLE_TABLE_LEVEL_METRICS;
import static org.apache.pinot.spi.utils.CommonConstants.Server.DEFAULT_METRICS_PREFIX;


/**
 * Utility class to centralize all metrics reporting for the Pinot server.
 *
 */
public class ServerMetrics extends AbstractMetrics<ServerQueryPhase, ServerMeter, ServerGauge, ServerTimer> {

  private static final AtomicReference<ServerMetrics> SERVER_METRICS_INSTANCE = new AtomicReference<>();

  /**
   * register the serverMetrics onto this class, so that we don't need to pass it down as a parameter
   */
  public static boolean register(ServerMetrics serverMetrics) {
    return SERVER_METRICS_INSTANCE.compareAndSet(null, serverMetrics);
  }

  @VisibleForTesting
  public static void deregister() {
    SERVER_METRICS_INSTANCE.set(null);
  }

  /**
   * should always call after registration
   */
  public static ServerMetrics get() {
    ServerMetrics ret = SERVER_METRICS_INSTANCE.get();
    assert ret != null;
    return ret;
  }

  public ServerMetrics(PinotMetricsRegistry metricsRegistry) {
    this(DEFAULT_METRICS_PREFIX, metricsRegistry, DEFAULT_ENABLE_TABLE_LEVEL_METRICS, Collections.emptySet());
  }

  public ServerMetrics(PinotMetricsRegistry metricsRegistry, boolean isTableLevelMetricsEnabled,
      Collection<String> allowedTables) {
    this(DEFAULT_METRICS_PREFIX, metricsRegistry, isTableLevelMetricsEnabled, allowedTables);
  }

  public ServerMetrics(String prefix, PinotMetricsRegistry metricsRegistry, boolean isTableLevelMetricsEnabled,
      Collection<String> allowedTables) {
    super(prefix, metricsRegistry, ServerMetrics.class, isTableLevelMetricsEnabled, allowedTables);
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

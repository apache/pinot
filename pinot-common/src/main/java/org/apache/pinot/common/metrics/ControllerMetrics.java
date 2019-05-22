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


/**
 * Metrics for the controller.
 */
public class ControllerMetrics extends AbstractMetrics<AbstractMetrics.QueryPhase, ControllerMeter, ControllerGauge, ControllerTimer> {

  // FYI this is not correct as it generate metrics named without a dot after pinot.controller part,
  // but we keep this default for backward compatibility in case someone relies on this format
  // see Servermetrics or BrokerMetrics class for correct prefix should be use
  public static final String METRICS_PREFIX_DEFAULT = "pinot.controller";

  public ControllerMetrics(MetricsRegistry metricsRegistry) {
    this(METRICS_PREFIX_DEFAULT, metricsRegistry);
  }

  public ControllerMetrics(String prefix, MetricsRegistry metricsRegistry) {
    super(prefix, metricsRegistry, ControllerMetrics.class);
  }

  @Override
  protected QueryPhase[] getQueryPhases() {
    return new QueryPhase[0];
  }

  @Override
  protected ControllerMeter[] getMeters() {
    return ControllerMeter.values();
  }

  @Override
  protected ControllerGauge[] getGauges() {
    return ControllerGauge.values();
  }
}

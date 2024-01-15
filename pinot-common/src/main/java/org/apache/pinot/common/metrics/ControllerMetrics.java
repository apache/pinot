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

import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;

import static org.apache.pinot.spi.utils.CommonConstants.Controller.DEFAULT_METRICS_PREFIX;


/**
 * Metrics for the controller.
 */
public class ControllerMetrics
    extends AbstractMetrics<AbstractMetrics.QueryPhase, ControllerMeter, ControllerGauge, ControllerTimer> {
  private static final AtomicReference<ControllerMetrics> CONTROLLER_METRICS_INSTANCE = new AtomicReference<>();

  public static boolean register(ControllerMetrics controllerMetrics) {
    return CONTROLLER_METRICS_INSTANCE.compareAndSet(null, controllerMetrics);
  }

  public static ControllerMetrics get() {
    return CONTROLLER_METRICS_INSTANCE.get();
  }

  public ControllerMetrics(PinotMetricsRegistry metricsRegistry) {
    this(DEFAULT_METRICS_PREFIX, metricsRegistry);
  }

  public ControllerMetrics(String prefix, PinotMetricsRegistry metricsRegistry) {
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

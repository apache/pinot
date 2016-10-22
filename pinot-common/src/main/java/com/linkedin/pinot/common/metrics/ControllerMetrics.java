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
 * Metrics for the controller.
 */
public class ControllerMetrics extends AbstractMetrics<AbstractMetrics.QueryPhase, ControllerMeter, ControllerGauge, ControllerTimer> {
  public ControllerMetrics(MetricsRegistry metricsRegistry) {
    super("pinot.controller", metricsRegistry, ControllerMetrics.class);
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

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
package com.linkedin.pinot.minion.metrics;

import com.linkedin.pinot.common.metrics.AbstractMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.yammer.metrics.core.MetricsRegistry;


public class MinionMetrics extends AbstractMetrics<MinionQueryPhase, MinionMeter, MinionGauge, MinionTimer> {

  public MinionMetrics(MetricsRegistry metricsRegistry) {
    super(CommonConstants.Minion.METRICS_PREFIX, metricsRegistry, MinionMetrics.class);
  }

  @Override
  protected MinionQueryPhase[] getQueryPhases() {
    return MinionQueryPhase.values();
  }

  @Override
  protected MinionMeter[] getMeters() {
    return MinionMeter.values();
  }

  @Override
  protected MinionGauge[] getGauges() {
    return MinionGauge.values();
  }
}

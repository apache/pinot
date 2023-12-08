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
import org.apache.pinot.spi.utils.CommonConstants;


public class MinionMetrics extends AbstractMetrics<MinionQueryPhase, MinionMeter, MinionGauge, MinionTimer> {
  private static final AtomicReference<MinionMetrics> MINION_METRICS_INSTANCE = new AtomicReference<>();

  public static boolean register(MinionMetrics minionMetrics) {
    return MINION_METRICS_INSTANCE.compareAndSet(null, minionMetrics);
  }

  public static MinionMetrics get() {
    return MINION_METRICS_INSTANCE.get();
  }

  public MinionMetrics(PinotMetricsRegistry metricsRegistry) {
    this(CommonConstants.Minion.CONFIG_OF_METRICS_PREFIX, metricsRegistry);
  }

  public MinionMetrics(String prefix, PinotMetricsRegistry metricsRegistry) {
    super(prefix, metricsRegistry, MinionMetrics.class);
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

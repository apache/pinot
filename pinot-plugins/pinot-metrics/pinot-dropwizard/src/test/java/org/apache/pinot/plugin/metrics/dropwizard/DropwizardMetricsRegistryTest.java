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
package org.apache.pinot.plugin.metrics.dropwizard;

import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotTimer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class DropwizardMetricsRegistryTest {
  @Test
  public void testNewGauge() {
    DropwizardMetricsRegistry dropwizardMetricsRegistry = new DropwizardMetricsRegistry();
    DropwizardSettableGauge<Long> dropwizardSettableGauge = new DropwizardSettableGauge<>(1L);
    DropwizardGauge<Long> dropwizardGauge = new DropwizardGauge<>(dropwizardSettableGauge);
    PinotGauge<Long> pinotGauge = dropwizardMetricsRegistry.newGauge(new DropwizardMetricName("test"), dropwizardGauge);
    assertEquals(pinotGauge.value(), Long.valueOf(1L));
    pinotGauge = dropwizardMetricsRegistry.newGauge(new DropwizardMetricName("test"), null);
    assertEquals(pinotGauge.value(), Long.valueOf(1L));
  }

  @Test
  public void testTimerReservoirIsBounded() {
    DropwizardMetricsRegistry registry = new DropwizardMetricsRegistry();
    PinotTimer timer = registry.newTimer(new DropwizardMetricName("timer"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    int updates = 10_000;
    for (int i = 0; i < updates; i++) {
      timer.update(1, TimeUnit.MILLISECONDS);
    }

    assertEquals(timer.count(), (long) updates);
    int retainedSamples = ((Timer) timer.getTimer()).getSnapshot().size();
    assertTrue(retainedSamples > 0 && retainedSamples <= 1028,
        "Timer retained " + retainedSamples + " samples after " + updates + " updates");
  }
}

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

import org.apache.pinot.spi.metrics.PinotGauge;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DropwizardMetricsRegistryTest {
  @Test
  public void testNewGauge() {
    DropwizardMetricsRegistry dropwizardMetricsRegistry = new DropwizardMetricsRegistry();
    DropwizardSettableGauge<Long> dropwizardSettableGauge = new DropwizardSettableGauge<>(1L);
    DropwizardGauge<Long> dropwizardGauge = new DropwizardGauge<>(dropwizardSettableGauge);
    PinotGauge<Long> pinotGauge = dropwizardMetricsRegistry.newGauge(new DropwizardMetricName("test"), dropwizardGauge);
    Assert.assertEquals(pinotGauge.value(), Long.valueOf(1L));
    pinotGauge = dropwizardMetricsRegistry.newGauge(new DropwizardMetricName("test"), null);
    Assert.assertEquals(pinotGauge.value(), Long.valueOf(1L));
  }
}

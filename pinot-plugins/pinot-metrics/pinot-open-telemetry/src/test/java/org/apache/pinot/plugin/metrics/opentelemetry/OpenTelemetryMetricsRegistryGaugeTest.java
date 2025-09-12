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
package org.apache.pinot.plugin.metrics.opentelemetry;

import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OpenTelemetryMetricsRegistryGaugeTest {

  @Test
  public void testNewGaugeGenerics() {
    OpenTelemetryMetricsFactory factory = new OpenTelemetryMetricsFactory();
    OpenTelemetryMetricsRegistry registry = new OpenTelemetryMetricsRegistry();
    PinotMetricsFactory.SimpleMetricName longGaugeName = new PinotMetricsFactory.SimpleMetricName("testLongGauge");
    PinotGauge<Long> pinotLongGauge = registry.newGauge(longGaugeName,
        factory.makePinotGauge(longGaugeName, (v) -> 1L));
    PinotMetricsFactory.SimpleMetricName doubleGaugeName = new PinotMetricsFactory.SimpleMetricName("testDoubleGauge");
    PinotGauge<Double> pinotDoubleGauge = registry.newGauge(doubleGaugeName,
        factory.makePinotGauge(doubleGaugeName, (v) -> 1.0));
    Assert.assertEquals(pinotLongGauge.value(), Long.valueOf(1L));
    Assert.assertEquals(pinotDoubleGauge.value(), Double.valueOf(1.0));
  }
}

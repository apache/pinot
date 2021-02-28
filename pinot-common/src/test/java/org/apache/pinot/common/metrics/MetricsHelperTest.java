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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the MetricsHelper class.
 *
 */
public class MetricsHelperTest {
  public static boolean listenerOneOkay;
  public static boolean listenerTwoOkay;

  public static class ListenerOne implements MetricsRegistryRegistrationListener {
    @Override
    public void onMetricsRegistryRegistered(PinotMetricsRegistry metricsRegistry) {
      listenerOneOkay = true;
    }
  }

  public static class ListenerTwo implements MetricsRegistryRegistrationListener {
    @Override
    public void onMetricsRegistryRegistered(PinotMetricsRegistry metricsRegistry) {
      listenerTwoOkay = true;
    }
  }

  @Test
  public void testMetricsHelperRegistration()
      throws InvalidConfigException {
    listenerOneOkay = false;
    listenerTwoOkay = false;

    Map<String, Object> properties = new HashMap<>();
    properties.put("pinot.broker.metrics.metricsRegistryRegistrationListeners",
        ListenerOne.class.getName() + "," + ListenerTwo.class.getName());

    PinotConfiguration configuration = new PinotConfiguration(properties);
    PinotMetricUtils.init(configuration);
    PinotMetricsRegistry registry = PinotMetricUtils.getPinotMetricsRegistry();

    // Initialize the MetricsHelper and create a new timer
    MetricsHelper.initializeMetrics(configuration.subset("pinot.broker.metrics"));
    MetricsHelper.registerMetricsRegistry(registry);
    MetricsHelper.newTimer(registry, PinotMetricUtils.generatePinotMetricName(MetricsHelperTest.class, "dummy"),
        TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS);

    // Check that the two listeners fired
    Assert.assertTrue(listenerOneOkay);
    Assert.assertTrue(listenerTwoOkay);
  }

  @Test
  public void testMetricValue() {
    PinotMetricsRegistry registry = PinotMetricUtils.getPinotMetricsRegistry();
    PinotMeter pinotMeter = MetricsHelper
        .newMeter(registry, PinotMetricUtils.generatePinotMetricName(MetricsHelperTest.class, "testMeter"), "testMeter",
            TimeUnit.MILLISECONDS);
    pinotMeter.mark();
    Assert.assertEquals(pinotMeter.count(), 1L);

    pinotMeter.mark(2L);
    Assert.assertEquals(pinotMeter.count(), 3L);
  }

  @Test
  public void testPinotMetricName() {
    PinotMetricName testMetricName1 =
        PinotMetricUtils.generatePinotMetricName(MetricsHelperTest.class, "testMetricName");
    PinotMetricName testMetricName2 =
        PinotMetricUtils.generatePinotMetricName(MetricsHelperTest.class, "testMetricName");
    Assert.assertNotNull(testMetricName1);
    Assert.assertNotNull(testMetricName2);
    Assert.assertEquals(testMetricName1, testMetricName2);
  }
}

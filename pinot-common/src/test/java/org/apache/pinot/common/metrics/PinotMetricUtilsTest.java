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
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotMetricUtilsTest {

  @Test
  public void testPinotMetricsRegistryFactory() {
    // Initialize a PinotMetricsRegistry without calling init() method.
    PinotMetricsRegistry pinotMetricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    Assert.assertNotNull(pinotMetricsRegistry);
    Assert.assertEquals(pinotMetricsRegistry.getClass().getSimpleName(), "YammerMetricsRegistry");

    try {
      // Initialize a PinotMetricsRegistry with calling init() method.
      Map<String, Object> properties = new HashMap<>();
      PinotConfiguration configuration = new PinotConfiguration(properties);
      PinotMetricUtils.init(configuration);
    } catch (Exception e) {
      Assert.fail("Fail to initialize PinotMetricsRegistry of yammer");
    }
    pinotMetricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    Assert.assertNotNull(pinotMetricsRegistry);
    Assert.assertEquals(pinotMetricsRegistry.getClass().getSimpleName(), "YammerMetricsRegistry");
  }

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
  public void testPinotMetricsRegistration()
      throws InvalidConfigException {
    listenerOneOkay = false;
    listenerTwoOkay = false;

    Map<String, Object> properties = new HashMap<>();
    properties.put("pinot.broker.metrics.metricsRegistryRegistrationListeners",
        PinotMetricUtilsTest.ListenerOne.class.getName() + "," + PinotMetricUtilsTest.ListenerTwo.class.getName());

    // Initialize the PinotMetricUtils and create a new timer
    PinotConfiguration configuration = new PinotConfiguration(properties);
    PinotMetricUtils.init(configuration.subset("pinot.broker.metrics"));
    PinotMetricsRegistry registry = PinotMetricUtils.getPinotMetricsRegistry();
    PinotMetricUtils.makePinotTimer(registry, PinotMetricUtils.makePinotMetricName(PinotMetricUtilsTest.class, "dummy"),
        TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS);

    // Check that the two listeners fired
    Assert.assertTrue(listenerOneOkay);
    Assert.assertTrue(listenerTwoOkay);
  }

  @Test
  public void testMetricValue() {
    PinotMetricsRegistry registry = PinotMetricUtils.getPinotMetricsRegistry();
    PinotMeter pinotMeter = PinotMetricUtils
        .makePinotMeter(registry, PinotMetricUtils.makePinotMetricName(PinotMetricUtilsTest.class, "testMeter"),
            "dummyEventType", TimeUnit.MILLISECONDS);
    pinotMeter.mark();
    Assert.assertEquals(pinotMeter.count(), 1L);

    pinotMeter.mark(2L);
    Assert.assertEquals(pinotMeter.count(), 3L);
  }

  @Test
  public void testPinotMetricName() {
    PinotMetricName testMetricName1 =
        PinotMetricUtils.makePinotMetricName(PinotMetricUtilsTest.class, "testMetricName");
    PinotMetricName testMetricName2 =
        PinotMetricUtils.makePinotMetricName(PinotMetricUtilsTest.class, "testMetricName");
    Assert.assertNotNull(testMetricName1);
    Assert.assertNotNull(testMetricName2);
    Assert.assertEquals(testMetricName1, testMetricName2);
  }
}

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
import org.apache.pinot.plugin.metrics.fake.FakeMetricsFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.MetricsRegistryRegistrationListener;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class PinotMetricUtilsTest {

  @AfterClass
  public void cleanUpMetricsFactory() {
    PinotMetricUtils.cleanUp();
  }

  @Test
  public void testPinotMetricsRegistryFactory() {
    try {
      Map<String, Object> properties = new HashMap<>();
      properties.put(CONFIG_OF_METRICS_FACTORY_CLASS_NAME, FakeMetricsFactory.class.getName());
      PinotConfiguration configuration = new PinotConfiguration(properties);
      PinotMetricUtils.init(configuration);
    } catch (Exception e) {
      Assert.fail("Fail to initialize PinotMetricsRegistry with the fake factory", e);
    }
    PinotMetricsRegistry pinotMetricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    Assert.assertNotNull(pinotMetricsRegistry);
    Assert.assertEquals(pinotMetricsRegistry.getClass().getSimpleName(), "FakePinotMetricsRegistry");
  }

  public static boolean _listenerOneOkay;
  public static boolean _listenerTwoOkay;

  public static class ListenerOne implements MetricsRegistryRegistrationListener {
    @Override
    public void onMetricsRegistryRegistered(PinotMetricsRegistry metricsRegistry) {
      _listenerOneOkay = true;
    }
  }

  public static class ListenerTwo implements MetricsRegistryRegistrationListener {
    @Override
    public void onMetricsRegistryRegistered(PinotMetricsRegistry metricsRegistry) {
      _listenerTwoOkay = true;
    }
  }

  @Test
  public void testPinotMetricsRegistration() {
    _listenerOneOkay = false;
    _listenerTwoOkay = false;

    Map<String, Object> properties = new HashMap<>();
    properties.put(CONFIG_OF_METRICS_FACTORY_CLASS_NAME, FakeMetricsFactory.class.getName());
    properties.put("pinot.broker.metrics.metricsRegistryRegistrationListeners",
        PinotMetricUtilsTest.ListenerOne.class.getName() + "," + PinotMetricUtilsTest.ListenerTwo.class.getName());

    PinotConfiguration configuration = new PinotConfiguration(properties);
    PinotMetricUtils.init(configuration.subset("pinot.broker.metrics"));
    PinotMetricUtils.init(configuration);
    PinotMetricsRegistry registry = PinotMetricUtils.getPinotMetricsRegistry();
    PinotMetricUtils.makePinotTimer(registry, PinotMetricUtils.makePinotMetricName(PinotMetricUtilsTest.class, "dummy"),
        TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS);

    Assert.assertTrue(_listenerOneOkay);
    Assert.assertTrue(_listenerTwoOkay);
  }

  @Test
  public void testMetricValue() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CONFIG_OF_METRICS_FACTORY_CLASS_NAME, FakeMetricsFactory.class.getName());
    PinotMetricUtils.init(new PinotConfiguration(properties));
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
    // Explicitly install the fake factory so this test doesn't rely on whichever factory a prior test left behind.
    Map<String, Object> properties = new HashMap<>();
    properties.put(CONFIG_OF_METRICS_FACTORY_CLASS_NAME, FakeMetricsFactory.class.getName());
    PinotMetricUtils.init(new PinotConfiguration(properties));

    PinotMetricName testMetricName1 =
        PinotMetricUtils.makePinotMetricName(PinotMetricUtilsTest.class, "testMetricName");
    PinotMetricName testMetricName2 =
        PinotMetricUtils.makePinotMetricName(PinotMetricUtilsTest.class, "testMetricName");
    Assert.assertNotNull(testMetricName1);
    Assert.assertNotNull(testMetricName2);
    Assert.assertEquals(testMetricName1, testMetricName2);
  }

  @Test
  public void testMetricRegistryFailure() {
    PinotMetricUtils.cleanUp();
    try {
      Map<String, Object> properties = new HashMap<>();
      properties.put("factory.className", "NonExistentClass");
      PinotConfiguration metricsConfiguration = new PinotConfiguration(properties);
      PinotMetricUtils.init(metricsConfiguration);
      Assert.fail("Illegal state exception should have been thrown since metrics factory class was not found");
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testCleanUp() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CONFIG_OF_METRICS_FACTORY_CLASS_NAME, FakeMetricsFactory.class.getName());
    PinotConfiguration config = new PinotConfiguration(properties);

    PinotMetricUtils.cleanUp();
    PinotMetricUtils.init(config);
    PinotMetricsRegistry registry = PinotMetricUtils.getPinotMetricsRegistry();
    PinotMetricsRegistry registry1 = PinotMetricUtils.getPinotMetricsRegistry();
    Assert.assertEquals(registry, registry1);
    PinotMetricUtils.cleanUp();
    // after cleaning up, a new one will be created
    PinotMetricUtils.init(config);
    PinotMetricsRegistry registry2 = PinotMetricUtils.getPinotMetricsRegistry();
    Assert.assertNotEquals(registry, registry2);
  }
}

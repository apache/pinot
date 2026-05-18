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
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotTimer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Verifies that {@link ServerMetricsEmitter} forwards emissions to the live {@link ServerMetrics}
 * singleton, resolving the singleton at call time so registration order between the two does not
 * affect correctness.
 */
public class ServerMetricsEmitterTest {

  @BeforeClass
  public void initMetricsFactory() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CONFIG_OF_METRICS_FACTORY_CLASS_NAME, FakeMetricsFactory.class.getName());
    PinotMetricUtils.init(new PinotConfiguration(properties));
  }

  @AfterClass
  public void cleanUpMetricsFactory() {
    PinotMetricUtils.cleanUp();
  }

  @BeforeMethod
  public void setUp() {
    ServerMetrics.deregister();
    ServerMetrics.register(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    MseMetricsEmitter.deregister();
    MseMetricsEmitter.register(new ServerMetricsEmitter());
  }

  @AfterMethod
  public void tearDown() {
    MseMetricsEmitter.deregister();
    ServerMetrics.deregister();
  }

  @Test
  public void testAddMeteredGlobalValueForwardsToServerMetrics() {
    long before = ServerMetrics.get().getMeteredValue(ServerMeter.MSE_QUERIES).count();
    MseMetricsEmitter.get().addMeteredGlobalValue(ServerMeter.MSE_QUERIES, 3L);
    long after = ServerMetrics.get().getMeteredValue(ServerMeter.MSE_QUERIES).count();
    assertEquals(after - before, 3L, "ServerMetricsEmitter should forward to the live ServerMetrics registry");
  }

  @Test
  public void testAddTimedValueForwardsToServerMetrics() {
    ServerMetrics serverMetrics = ServerMetrics.get();
    ServerTimer timerKey = ServerTimer.MULTI_STAGE_SERIALIZATION_CPU_TIME_MS;
    String fullTimerName = "pinot.server." + timerKey.getTimerName();

    // Resolve the same timer handle the emitter writes to. PinotMetricUtils#makePinotTimer is
    // idempotent — it returns the existing timer if one is registered under the name.
    PinotTimer timer = PinotMetricUtils.makePinotTimer(serverMetrics.getMetricsRegistry(),
        PinotMetricUtils.makePinotMetricName(ServerMetrics.class, fullTimerName),
        TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    long before = timer.count();

    MseMetricsEmitter.get().addTimedValue(timerKey, 50L, TimeUnit.MILLISECONDS);

    assertEquals(timer.count() - before, 1L,
        "ServerMetricsEmitter should forward addTimedValue to the live ServerMetrics timer");
  }

  @Test
  public void testGetMeteredValueIsBoundToActiveRegistry() {
    PinotMeter meter = MseMetricsEmitter.get().getMeteredValue(ServerMeter.MSE_OPCHAINS_STARTED);
    assertNotNull(meter, "getMeteredValue must return a non-null handle");

    long before = ServerMetrics.get().getMeteredValue(ServerMeter.MSE_OPCHAINS_STARTED).count();
    meter.mark();
    meter.mark(4L);
    long after = ServerMetrics.get().getMeteredValue(ServerMeter.MSE_OPCHAINS_STARTED).count();
    assertEquals(after - before, 5L,
        "Marking the meter handle returned by the emitter must increment the live ServerMetrics counter");
  }

  @Test
  public void testLateServerMetricsRegistrationStillEmitsCorrectly() {
    // Deregister both, install ONLY the emitter, then register a fresh ServerMetrics.
    // The emitter resolves ServerMetrics.get() at call time, so the late registration must take
    // effect transparently. This is the test that protects against the NOOP-binding hazard.
    MseMetricsEmitter.deregister();
    ServerMetrics.deregister();
    MseMetricsEmitter.register(new ServerMetricsEmitter());

    // No ServerMetrics registered yet — emission should silently land on the NOOP registry.
    MseMetricsEmitter.get().addMeteredGlobalValue(ServerMeter.MSE_QUERIES, 99L);

    // Register a real ServerMetrics now.
    ServerMetrics late = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    ServerMetrics.register(late);

    long before = ServerMetrics.get().getMeteredValue(ServerMeter.MSE_QUERIES).count();
    MseMetricsEmitter.get().addMeteredGlobalValue(ServerMeter.MSE_QUERIES, 11L);
    long after = ServerMetrics.get().getMeteredValue(ServerMeter.MSE_QUERIES).count();
    assertEquals(after - before, 11L,
        "Emissions after late ServerMetrics registration must land on the new registry");
  }
}

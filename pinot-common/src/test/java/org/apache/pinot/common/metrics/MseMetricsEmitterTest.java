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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for the {@link MseMetricsEmitter} singleton lifecycle and NOOP behavior.
 */
public class MseMetricsEmitterTest {

  @AfterMethod
  public void tearDown() {
    MseMetricsEmitter.deregister();
  }

  @Test
  public void testGetReturnsNonNullNoopBeforeRegistration() {
    MseMetricsEmitter.deregister();
    MseMetricsEmitter emitter = MseMetricsEmitter.get();
    assertNotNull(emitter, "get() must return a non-null emitter even before registration");
    assertSame(emitter, MseMetricsEmitterHolder.NOOP, "Pre-registration get() should return the NOOP sentinel");
  }

  @Test
  public void testRegisterFirstWriteWins() {
    MseMetricsEmitter first = new RecordingEmitter();
    MseMetricsEmitter second = new RecordingEmitter();
    assertTrue(MseMetricsEmitter.register(first), "first register() must succeed");
    assertFalse(MseMetricsEmitter.register(second), "subsequent register() must return false");
    assertSame(MseMetricsEmitter.get(), first, "get() must return the first registered emitter");
  }

  @Test
  public void testDeregisterResetsToNoop() {
    MseMetricsEmitter emitter = new RecordingEmitter();
    MseMetricsEmitter.register(emitter);
    assertSame(MseMetricsEmitter.get(), emitter);
    MseMetricsEmitter.deregister();
    assertSame(MseMetricsEmitter.get(), MseMetricsEmitterHolder.NOOP);
  }

  @Test
  public void testNoopEmissionDoesNotThrow() {
    MseMetricsEmitter.deregister();
    MseMetricsEmitter emitter = MseMetricsEmitter.get();
    emitter.addMeteredGlobalValue(ServerMeter.MSE_QUERIES, 42L);
    emitter.addTimedValue(ServerTimer.MULTI_STAGE_SERIALIZATION_CPU_TIME_MS, 100L, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testNoopGetMeteredValueReturnsUsableMeter() {
    MseMetricsEmitter.deregister();
    PinotMeter meter = MseMetricsEmitter.get().getMeteredValue(ServerMeter.MULTI_STAGE_RUNNER_STARTED_TASKS);
    assertNotNull(meter, "NOOP getMeteredValue() must return a non-null meter handle");
    // Should be safe to call mark()/count() — values are ignored but no exception expected
    meter.mark();
    meter.mark(7L);
    assertEquals(meter.count(), 0L, "NOOP meter count() always returns 0");
  }

  /**
   * Minimal {@link MseMetricsEmitter} that records the last invocation arguments for assertions.
   */
  private static final class RecordingEmitter implements MseMetricsEmitter {
    final AtomicLong _lastUnitCount = new AtomicLong();
    final AtomicLong _lastTimedDuration = new AtomicLong();

    @Override
    public void addMeteredGlobalValue(ServerMeter meter, long unitCount) {
      _lastUnitCount.set(unitCount);
    }

    @Override
    public PinotMeter getMeteredValue(ServerMeter meter) {
      return MseMetricsEmitterHolder.NOOP.getMeteredValue(meter);
    }

    @Override
    public void addTimedValue(ServerTimer timer, long duration, TimeUnit timeUnit) {
      _lastTimedDuration.set(duration);
    }
  }
}

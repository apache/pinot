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
package org.apache.pinot.core.data.manager.realtime;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class RealtimeIngestionMemoryGuardTest {
  private static final long MAX_HEAP = 1000L;

  private RealtimeIngestionMemoryGuard newGuard(AtomicLong usedHeap, AtomicLong clockMs) {
    return new RealtimeIngestionMemoryGuard(usedHeap::get, () -> MAX_HEAP, clockMs::get);
  }

  private PinotConfiguration config(Map<String, Object> overrides) {
    Map<String, Object> map = new HashMap<>(overrides);
    return new PinotConfiguration(map);
  }

  private Map<String, Object> baseConfig(String mode) {
    Map<String, Object> map = new HashMap<>();
    map.put(Server.CONFIG_OF_SERVER_CONSUMPTION_MEMORY_PAUSE_MODE, mode);
    map.put(Server.CONFIG_OF_SERVER_CONSUMPTION_MEMORY_PAUSE_HEAP_RATIO, 0.85);
    map.put(Server.CONFIG_OF_SERVER_CONSUMPTION_MEMORY_RESUME_HEAP_RATIO, 0.75);
    return map;
  }

  @Test
  public void testHeapPressureHysteresis() {
    AtomicLong usedHeap = new AtomicLong(0);
    AtomicLong clock = new AtomicLong(1000);
    RealtimeIngestionMemoryGuard guard = newGuard(usedHeap, clock);
    guard.applyConfig(config(baseConfig("ALL")));

    // Just below pause ratio (0.849 < 0.85) -> no pressure
    usedHeap.set(849);
    guard.sampleOnce();
    assertFalse(guard.isHeapPressure());

    // Exactly at the pause ratio (0.85, inclusive) -> pressure
    usedHeap.set(850);
    guard.sampleOnce();
    assertTrue(guard.isHeapPressure());

    // In the hysteresis band (between resume and pause ratios) -> stays paused
    usedHeap.set(800);
    guard.sampleOnce();
    assertTrue(guard.isHeapPressure());

    // Just above the resume ratio (0.751 > 0.75) -> stays paused
    usedHeap.set(751);
    guard.sampleOnce();
    assertTrue(guard.isHeapPressure());

    // Exactly at the resume ratio (0.75, inclusive) -> resumes
    usedHeap.set(750);
    guard.sampleOnce();
    assertFalse(guard.isHeapPressure());
  }

  @Test
  public void testUnknownMaxHeapDoesNotPause() {
    AtomicLong usedHeap = new AtomicLong(950);
    AtomicLong clock = new AtomicLong(1000);
    // maxHeap reported as 0 (unknown) -> cannot compute a ratio, must not flip to pressure or throw
    RealtimeIngestionMemoryGuard guard = new RealtimeIngestionMemoryGuard(usedHeap::get, () -> 0L, clock::get);
    guard.applyConfig(config(baseConfig("ALL")));
    guard.sampleOnce();
    assertFalse(guard.isHeapPressure());
    assertFalse(guard.shouldPauseConsumption(null, null));
  }

  @Test
  public void testDisabledModeNeverPauses() {
    AtomicLong usedHeap = new AtomicLong(950);
    AtomicLong clock = new AtomicLong(1000);
    RealtimeIngestionMemoryGuard guard = newGuard(usedHeap, clock);
    guard.applyConfig(config(baseConfig("DISABLED")));
    guard.sampleOnce();

    PartitionUpsertMetadataManager upsert = mock(PartitionUpsertMetadataManager.class);
    assertFalse(guard.shouldPauseConsumption(upsert, null));
    assertFalse(guard.shouldPauseConsumption(null, null));
  }

  @Test
  public void testUpsertDedupOnlyModeEligibility() {
    AtomicLong usedHeap = new AtomicLong(950);
    AtomicLong clock = new AtomicLong(1000);
    RealtimeIngestionMemoryGuard guard = newGuard(usedHeap, clock);
    guard.applyConfig(config(baseConfig("UPSERT_DEDUP_ONLY")));
    guard.sampleOnce();
    assertTrue(guard.isHeapPressure());

    PartitionUpsertMetadataManager upsert = mock(PartitionUpsertMetadataManager.class);
    PartitionDedupMetadataManager dedup = mock(PartitionDedupMetadataManager.class);

    // Upsert or dedup tables are eligible and pause under heap pressure
    assertTrue(guard.shouldPauseConsumption(upsert, null));
    assertTrue(guard.shouldPauseConsumption(null, dedup));
    // Non-upsert, non-dedup table is not eligible in this mode
    assertFalse(guard.shouldPauseConsumption(null, null));
  }

  @Test
  public void testAllModePausesAnyTable() {
    AtomicLong usedHeap = new AtomicLong(950);
    AtomicLong clock = new AtomicLong(1000);
    RealtimeIngestionMemoryGuard guard = newGuard(usedHeap, clock);
    guard.applyConfig(config(baseConfig("ALL")));
    guard.sampleOnce();
    assertTrue(guard.isHeapPressure());

    // A plain realtime table (no upsert/dedup managers) pauses under heap pressure in ALL mode
    assertTrue(guard.shouldPauseConsumption(null, null));
  }

  @Test
  public void testFailOpenOnStaleHeartbeat() {
    AtomicLong usedHeap = new AtomicLong(950);
    AtomicLong clock = new AtomicLong(1000);
    RealtimeIngestionMemoryGuard guard = newGuard(usedHeap, clock);
    guard.applyConfig(config(baseConfig("ALL")));
    guard.sampleOnce();
    assertTrue(guard.isHeapPressure());
    assertTrue(guard.shouldPauseConsumption(null, null));

    // Advance the clock beyond the staleness threshold (5 * checkInterval). The sampler has not refreshed, so the
    // guard must fail open and stop pausing even though the (stuck) pressure flag is still true.
    clock.addAndGet(5 * Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_CHECK_INTERVAL_MS + 1);
    assertTrue(guard.isHeapPressure());
    assertFalse(guard.shouldPauseConsumption(null, null));
  }

  @Test
  public void testInvalidRatiosFallBackToDefaults() {
    AtomicLong usedHeap = new AtomicLong(0);
    AtomicLong clock = new AtomicLong(1000);
    RealtimeIngestionMemoryGuard guard = newGuard(usedHeap, clock);
    Map<String, Object> cfg = baseConfig("ALL");
    // resume >= pause is invalid (no hysteresis band) -> the default 0.85/0.75 band must be used instead.
    cfg.put(Server.CONFIG_OF_SERVER_CONSUMPTION_MEMORY_PAUSE_HEAP_RATIO, 0.5);
    cfg.put(Server.CONFIG_OF_SERVER_CONSUMPTION_MEMORY_RESUME_HEAP_RATIO, 0.9);
    guard.applyConfig(config(cfg));

    // 0.80 is above the rejected pause ratio (0.5) but below the default (0.85): no pressure proves the default is in
    // effect.
    usedHeap.set(800);
    guard.sampleOnce();
    assertFalse(guard.isHeapPressure());
    // 0.86 crosses the default pause ratio.
    usedHeap.set(860);
    guard.sampleOnce();
    assertTrue(guard.isHeapPressure());
  }

  @Test
  public void testInvalidModeFallsBackToDefault() {
    AtomicLong usedHeap = new AtomicLong(0);
    AtomicLong clock = new AtomicLong(1000);
    RealtimeIngestionMemoryGuard guard = newGuard(usedHeap, clock);
    Map<String, Object> cfg = baseConfig("NOT_A_MODE");
    guard.applyConfig(config(cfg));

    assertEquals(guard.getMode().name(), Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_PAUSE_MODE);
  }
}

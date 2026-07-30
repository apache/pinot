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
package org.apache.pinot.materializedview.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.zookeeper.data.Stat;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Unit tests for [MaterializedViewPartitionManager].  Covers each public state-change
/// op's strict / lenient precondition and the CAS engine's retry classification.
public class MaterializedViewPartitionManagerTest {
  private static final String TABLE = "mv_orders_OFFLINE";
  private static final long BUCKET_MS = 86_400_000L;
  private static final long W0 = 1_700_000_000_000L;
  private static final long W1 = W0 + BUCKET_MS;
  private static final long W2 = W0 + 2 * BUCKET_MS;
  private static final PartitionFingerprint FP1 = new PartitionFingerprint(3, 0xCAFEL);
  private static final PartitionFingerprint FP2 = new PartitionFingerprint(5, 0xBEEFL);

  // ─────────────────────────────────────────────────────────────────────────────────────
  //  appendValid
  // ─────────────────────────────────────────────────────────────────────────────────────

  @Test
  public void testAppendValidVacantToValidAdvancesWatermark() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    stubFetch(store, runtime(W0, existing), 7);
    stubSet(store, true);

    MaterializedViewPartitionManager mgr = new MaterializedViewPartitionManager(store, null);
    mgr.appendValid(TABLE, W0, W1, FP1);

    MaterializedViewRuntimeMetadata persisted = capturePersisted(store, 7);
    assertEquals(persisted.getWatermarkMs(), W1, "watermark advances by one bucket");
    PartitionInfo info = persisted.getPartitions().get(W0);
    assertNotNull(info);
    assertEquals(info.getState(), PartitionState.VALID);
    assertEquals(info.getFingerprint(), FP1);
  }

  @Test
  public void testAppendValidWatermarkClimbsContiguousChain() {
    // Map already has W1 VALID (from a prior batch); appending W0 should advance watermark
    // through both contiguous buckets.
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W1, new PartitionInfo(PartitionState.VALID, FP2, 1L));
    stubFetch(store, runtime(W0, existing), 1);
    stubSet(store, true);

    new MaterializedViewPartitionManager(store, null).appendValid(TABLE, W0, W1, FP1);

    assertEquals(capturePersisted(store, 1).getWatermarkMs(), W2,
        "watermark walks past contiguous VALID chain");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testAppendValidStrictRejectsExistingBucket() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.VALID, FP2, 1L));
    stubFetch(store, runtime(W0, existing), 0);

    new MaterializedViewPartitionManager(store, null).appendValid(TABLE, W0, W1, FP1);
  }

  @Test
  public void testAppendValidIdempotentOnLostAckReplay() {
    // Lost-ack replay: ZK committed our appendValid write but the client saw a connection drop,
    // so the CAS loop re-runs the mutator against our own committed state (bucket VALID, same
    // fingerprint, watermark already advanced).  Must no-op successfully — NOT throw the strict
    // "bucket already present" violation, which would hard-fail a minion task that succeeded.
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.VALID, FP1, 1L));
    stubFetch(store, runtime(W1, existing), 3);

    new MaterializedViewPartitionManager(store, null).appendValid(TABLE, W0, W1, FP1);

    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testAppendValidThrowsWhenRuntimeMissing() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    stubFetchAbsent(store);

    new MaterializedViewPartitionManager(store, null).appendValid(TABLE, W0, W1, FP1);
  }

  // ─────────────────────────────────────────────────────────────────────────────────────
  //  refreshValid
  // ─────────────────────────────────────────────────────────────────────────────────────

  @Test
  public void testRefreshValidStaleToValidUpdatesFingerprint() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.STALE, FP2, 1L));
    stubFetch(store, runtime(W1, existing), 4);
    stubSet(store, true);

    new MaterializedViewPartitionManager(store, null).refreshValid(TABLE, W0, FP1);

    MaterializedViewRuntimeMetadata persisted = capturePersisted(store, 4);
    assertEquals(persisted.getWatermarkMs(), W1, "watermark unchanged on OVERWRITE");
    PartitionInfo info = persisted.getPartitions().get(W0);
    assertEquals(info.getState(), PartitionState.VALID);
    assertEquals(info.getFingerprint(), FP1);
    assertTrue(info.getLastRefreshTime() > 1L, "lastRefreshTime is bumped to now");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testRefreshValidStrictRejectsAbsentBucket() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    stubFetch(store, runtime(W0, new HashMap<>()), 0);

    new MaterializedViewPartitionManager(store, null).refreshValid(TABLE, W0, FP1);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testRefreshValidStrictRejectsValidBucketWithDifferentFingerprint() {
    // VALID with a DIFFERENT fingerprint is a genuine dispatch-invariant violation (not a
    // lost-ack replay of our own write) and must still fail loud.
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.VALID, FP2, 1L));
    stubFetch(store, runtime(W0, existing), 0);

    new MaterializedViewPartitionManager(store, null).refreshValid(TABLE, W0, FP1);
  }

  @Test
  public void testRefreshValidIdempotentOnLostAckReplay() {
    // Same lost-ack shape as testAppendValidIdempotentOnLostAckReplay, for OVERWRITE commits.
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.VALID, FP1, 1L));
    stubFetch(store, runtime(W1, existing), 5);

    new MaterializedViewPartitionManager(store, null).refreshValid(TABLE, W0, FP1);

    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  // ─────────────────────────────────────────────────────────────────────────────────────
  //  clearValid
  // ─────────────────────────────────────────────────────────────────────────────────────

  @Test
  public void testClearValidStaleToValidEmptyPersistsEmptyFingerprint() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.STALE, FP2, 1L));
    stubFetch(store, runtime(W0, existing), 2);
    stubSet(store, true);

    // EMPTY fingerprint => source confirmed still empty at commit => write VALID-empty.
    new MaterializedViewPartitionManager(store, null).clearValid(TABLE, W0, () -> PartitionFingerprint.EMPTY);

    PartitionInfo info = capturePersisted(store, 2).getPartitions().get(W0);
    assertEquals(info.getState(), PartitionState.VALID);
    assertEquals(info.getFingerprint(), PartitionFingerprint.EMPTY);
  }

  @Test
  public void testClearValidLeavesStaleWhenSourceBackfilled() {
    // Backfill race: the scheduler dispatched DELETE on an empty source, but a backfill landed
    // before commit (the recomputed source fingerprint has overlapping segments).  clearValid
    // must NOT write VALID-empty — it leaves the bucket STALE so the next cycle re-materializes
    // it via OVERWRITE.  Writing VALID-empty here would silently drop the backfilled rows.
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.STALE, FP2, 1L));
    stubFetch(store, runtime(W0, existing), 2);
    stubSet(store, true);

    // Non-empty fingerprint (segmentCount=3) => source backfilled => no-op, bucket stays STALE.
    new MaterializedViewPartitionManager(store, null).clearValid(TABLE, W0, () -> FP1);

    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  @Test
  public void testClearValidReReadsSourceOnEachCasAttempt() {
    // Race the supplier across a CAS conflict: the source is empty on attempt 1 (so the mutator
    // builds a VALID-empty record) but the write loses a version race; a backfill lands before the
    // retry, so on attempt 2 the supplier reports a non-empty source and clearValid must no-op,
    // leaving the bucket STALE.  This pins the fix: the source emptiness is re-evaluated inside the
    // CAS loop, not snapshotted once before it (which would write VALID-empty over the backfill).
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.STALE, FP2, 1L));
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(TABLE);

    AtomicInteger version = new AtomicInteger(0);
    when(store.get(eq(path), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenAnswer((InvocationOnMock inv) -> {
          Stat stat = inv.getArgument(1);
          stat.setVersion(version.get());
          return runtime(W0, existing).toZNRecord();
        });
    // Every write loses the version race (simulating a concurrent writer), forcing a retry.
    AtomicInteger setCalls = new AtomicInteger(0);
    when(store.set(eq(path), any(ZNRecord.class), anyInt(), eq(AccessOption.PERSISTENT)))
        .thenAnswer(inv -> {
          setCalls.incrementAndGet();
          version.incrementAndGet();
          return false;
        });

    // Supplier: empty on attempt 1, non-empty (backfill landed) on every later attempt.
    AtomicInteger supplierCalls = new AtomicInteger(0);
    Supplier<PartitionFingerprint> supplier =
        () -> supplierCalls.incrementAndGet() == 1 ? PartitionFingerprint.EMPTY : FP1;

    new MaterializedViewPartitionManager(store, null).clearValid(TABLE, W0, supplier);

    assertTrue(supplierCalls.get() >= 2, "supplier must be re-invoked on the CAS retry");
    assertEquals(setCalls.get(), 1,
        "only attempt 1 (empty source) tried to write; attempt 2 saw the backfill and no-oped");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testClearValidStrictRejectsAbsentBucket() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    stubFetch(store, runtime(W0, new HashMap<>()), 0);

    // Strict precondition (present & STALE) is checked before the emptiness guard, so an absent
    // bucket still throws regardless of the supplied fingerprint.
    new MaterializedViewPartitionManager(store, null).clearValid(TABLE, W0, () -> PartitionFingerprint.EMPTY);
  }

  @Test
  public void testClearValidIdempotentOnLostAckReplay() {
    // Lost-ack replay of a committed VALID-empty write: must no-op without re-invoking the
    // source-emptiness supplier (a backfill landing after the committed write is the periodic
    // sweep's responsibility, not this replay's).
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.VALID, PartitionFingerprint.EMPTY, 1L));
    stubFetch(store, runtime(W0, existing), 4);

    AtomicInteger supplierCalls = new AtomicInteger(0);
    new MaterializedViewPartitionManager(store, null).clearValid(TABLE, W0, () -> {
      supplierCalls.incrementAndGet();
      return PartitionFingerprint.EMPTY;
    });

    assertEquals(supplierCalls.get(), 0, "idempotent replay must not re-read the source");
    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  // ─────────────────────────────────────────────────────────────────────────────────────
  //  revertValid
  // ─────────────────────────────────────────────────────────────────────────────────────

  @Test
  public void testRevertValidStaleToValidPreservesFingerprintAndTimestamp() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    PartitionInfo stale = new PartitionInfo(PartitionState.STALE, FP2, 12345L);
    existing.put(W0, stale);
    stubFetch(store, runtime(W1, existing), 9);
    stubSet(store, true);

    new MaterializedViewPartitionManager(store, null).revertValid(TABLE, W0);

    PartitionInfo info = capturePersisted(store, 9).getPartitions().get(W0);
    assertEquals(info.getState(), PartitionState.VALID);
    assertEquals(info.getFingerprint(), FP2, "fingerprint preserved");
    assertEquals(info.getLastRefreshTime(), 12345L, "lastRefreshTime preserved");
  }

  @Test
  public void testRevertValidLenientNoOpWhenNotStale() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.VALID, FP2, 1L));
    stubFetch(store, runtime(W1, existing), 0);

    new MaterializedViewPartitionManager(store, null).revertValid(TABLE, W0);

    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  @Test
  public void testRevertValidLenientNoOpWhenAbsent() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    stubFetch(store, runtime(W1, new HashMap<>()), 0);

    new MaterializedViewPartitionManager(store, null).revertValid(TABLE, W0);

    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  @Test
  public void testRevertValidLenientNoOpWhenRuntimeMissing() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    stubFetchAbsent(store);

    new MaterializedViewPartitionManager(store, null).revertValid(TABLE, W0);

    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  // ─────────────────────────────────────────────────────────────────────────────────────
  //  markStale
  // ─────────────────────────────────────────────────────────────────────────────────────

  @Test
  public void testMarkStaleValidToStaleSingleBucket() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    PartitionInfo valid = new PartitionInfo(PartitionState.VALID, FP1, 1L);
    existing.put(W0, valid);
    stubFetch(store, runtime(W1, existing), 5);
    stubSet(store, true);

    new MaterializedViewPartitionManager(store, null).markStale(TABLE, W0);

    PartitionInfo info = capturePersisted(store, 5).getPartitions().get(W0);
    assertEquals(info.getState(), PartitionState.STALE);
    assertEquals(info.getFingerprint(), FP1, "fingerprint preserved on markStale");
  }

  @Test
  public void testMarkStaleBatchSingleCasOnly() {
    // Three buckets: VALID/STALE/absent.  Manager flips only the VALID one and writes once.
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.VALID, FP1, 1L));
    existing.put(W1, new PartitionInfo(PartitionState.STALE, FP2, 1L));
    stubFetch(store, runtime(W2, existing), 3);
    stubSet(store, true);

    new MaterializedViewPartitionManager(store, null).markStale(TABLE, List.of(W0, W1, W2));

    verify(store, times(1)).set(anyString(), any(ZNRecord.class), eq(3), eq(AccessOption.PERSISTENT));
    Map<Long, PartitionInfo> persisted = capturePersisted(store, 3).getPartitions();
    assertEquals(persisted.get(W0).getState(), PartitionState.STALE, "VALID bucket flipped");
    assertEquals(persisted.get(W1).getState(), PartitionState.STALE, "already-STALE bucket unchanged");
    assertFalse(persisted.containsKey(W2), "absent bucket not synthesized today");
  }

  @Test
  public void testMarkStaleNoOpWhenNoValidEntries() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.STALE, FP1, 1L));
    stubFetch(store, runtime(W1, existing), 0);

    new MaterializedViewPartitionManager(store, null).markStale(TABLE, List.of(W0));

    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  @Test
  public void testMarkStaleEmptyCollectionShortCircuits() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);

    new MaterializedViewPartitionManager(store, null).markStale(TABLE, List.of());

    verify(store, never()).get(anyString(), any(Stat.class), anyInt());
    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  @Test
  public void testMarkStaleNoOpWhenRuntimeMissing() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    stubFetchAbsent(store);

    new MaterializedViewPartitionManager(store, null).markStale(TABLE, List.of(W0));

    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  // ─────────────────────────────────────────────────────────────────────────────────────
  //  deletePartition
  // ─────────────────────────────────────────────────────────────────────────────────────

  @Test
  public void testDeletePartitionRemovesEntry() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.STALE, FP1, 1L));
    existing.put(W1, new PartitionInfo(PartitionState.VALID, FP2, 1L));
    stubFetch(store, runtime(W1, existing), 6);
    stubSet(store, true);

    new MaterializedViewPartitionManager(store, null).deletePartition(TABLE, W0);

    Map<Long, PartitionInfo> persisted = capturePersisted(store, 6).getPartitions();
    assertFalse(persisted.containsKey(W0), "deleted bucket no longer present");
    assertTrue(persisted.containsKey(W1), "untouched bucket remains");
  }

  @Test
  public void testDeletePartitionNoOpWhenAbsent() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    stubFetch(store, runtime(W0, new HashMap<>()), 0);

    new MaterializedViewPartitionManager(store, null).deletePartition(TABLE, W0);

    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  @Test
  public void testDeletePartitionNoOpWhenRuntimeMissing() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    stubFetchAbsent(store);

    new MaterializedViewPartitionManager(store, null).deletePartition(TABLE, W0);

    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  // ─────────────────────────────────────────────────────────────────────────────────────
  //  CAS retry classification
  // ─────────────────────────────────────────────────────────────────────────────────────

  @Test
  public void testCasConflictRetriesUntilSuccess() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.STALE, FP2, 1L));
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(TABLE);

    AtomicInteger version = new AtomicInteger(0);
    when(store.get(eq(path), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenAnswer((InvocationOnMock inv) -> {
          Stat stat = inv.getArgument(1);
          stat.setVersion(version.get());
          return runtime(W1, existing).toZNRecord();
        });

    AtomicInteger setCalls = new AtomicInteger(0);
    when(store.set(eq(path), any(ZNRecord.class), anyInt(), eq(AccessOption.PERSISTENT)))
        .thenAnswer(inv -> {
          int callIdx = setCalls.incrementAndGet();
          if (callIdx <= 2) {
            // Simulate concurrent writer winning — bump the stored version.
            version.incrementAndGet();
            return false;
          }
          return true;
        });

    new MaterializedViewPartitionManager(store, null).revertValid(TABLE, W0);

    assertEquals(setCalls.get(), 3, "should have retried twice and succeeded on the third attempt");
  }

  @Test
  public void testReadSideZkExceptionIsRetried() {
    // A transient read-side ZK failure on the first attempt must be counted against the retry
    // budget and re-fetched, not escape unretried; the second read succeeds and the op commits.
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.STALE, FP2, 1L));
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(TABLE);

    when(store.get(eq(path), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenThrow(new ZkException("transient read failure"))
        .thenAnswer((InvocationOnMock inv) -> {
          Stat stat = inv.getArgument(1);
          stat.setVersion(0);
          return runtime(W0, existing).toZNRecord();
        });
    stubSet(store, true);

    new MaterializedViewPartitionManager(store, null).revertValid(TABLE, W0);

    // Recovered after the read retry: exactly one successful set (no exception escaped).
    verify(store, times(1)).set(eq(path), any(ZNRecord.class), anyInt(), eq(AccessOption.PERSISTENT));
  }

  @Test
  public void testMutatorPreconditionViolationFailsFastWithoutRetry() {
    // The retry-classification contract: an IllegalStateException thrown by the mutator (a
    // precondition violation — same fail-fast class as a validateForPersist rejection, which is
    // currently a documented no-op) must propagate on the FIRST attempt.  Burning the critical
    // retry budget (128 attempts x backoff) on a deterministic violation would mask the
    // actionable error for ~25 s and hammer ZK with useless re-fetches.
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.VALID, FP2, 1L));
    stubFetch(store, runtime(W0, existing), 0);

    try {
      // VALID bucket with a different fingerprint => strict refreshValid violation.
      new MaterializedViewPartitionManager(store, null).refreshValid(TABLE, W0, FP1);
      fail("Expected IllegalStateException for the strict precondition violation");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("STALE"),
          "message should describe the violated precondition; got: " + e.getMessage());
    }
    // Exactly one fetch (no retry burn) and no write.
    verify(store, times(1)).get(anyString(), any(Stat.class), anyInt());
    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  @Test
  public void testRevertBudgetExhaustedThrowsRuntimeException() {
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(W0, new PartitionInfo(PartitionState.STALE, FP2, 1L));
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(TABLE);

    when(store.get(eq(path), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenAnswer((InvocationOnMock inv) -> {
          Stat stat = inv.getArgument(1);
          stat.setVersion(0);
          return runtime(W1, existing).toZNRecord();
        });
    when(store.set(anyString(), any(ZNRecord.class), anyInt(), anyInt()))
        .thenReturn(false);

    try {
      new MaterializedViewPartitionManager(store, null).revertValid(TABLE, W0);
      fail("Expected RuntimeException after exhausting revert budget");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains(TABLE),
          "Exception message must include the offending table name; got: " + e.getMessage());
    }
    verify(store, times(MaterializedViewPartitionManager.REVERT_MAX_ATTEMPTS))
        .set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  @Test
  public void testReadSideZkExceptionBudgetExhaustedThrows() {
    // Read fails on EVERY attempt: the read-side ZkException is retried up to the budget and then
    // surfaces as a RuntimeException wrapping the last ZkException — symmetric with the write-side
    // budget-exhaustion path above, so a persistent read fault is never silently swallowed.
    HelixPropertyStore<ZNRecord> store = mock(HelixPropertyStore.class);
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(TABLE);
    when(store.get(eq(path), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenThrow(new ZkException("persistent read failure"));

    try {
      new MaterializedViewPartitionManager(store, null).revertValid(TABLE, W0);
      fail("Expected RuntimeException after read-side budget exhaustion");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains(TABLE),
          "Exception message must include the offending table name; got: " + e.getMessage());
    }
    verify(store, times(MaterializedViewPartitionManager.REVERT_MAX_ATTEMPTS))
        .get(eq(path), any(Stat.class), eq(AccessOption.PERSISTENT));
    verify(store, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  @Test
  public void testNullPropertyStoreRejected() {
    try {
      new MaterializedViewPartitionManager(null, null);
      fail("Expected IllegalArgumentException for null propertyStore");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("propertyStore"));
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────────────
  //  Helpers
  // ─────────────────────────────────────────────────────────────────────────────────────

  private static MaterializedViewRuntimeMetadata runtime(long watermarkMs,
      Map<Long, PartitionInfo> partitions) {
    return new MaterializedViewRuntimeMetadata(TABLE, watermarkMs, partitions);
  }

  private static void stubFetch(HelixPropertyStore<ZNRecord> store,
      MaterializedViewRuntimeMetadata snapshot, int version) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(TABLE);
    when(store.get(eq(path), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenAnswer((InvocationOnMock inv) -> {
          Stat stat = inv.getArgument(1);
          stat.setVersion(version);
          return snapshot.toZNRecord();
        });
  }

  private static void stubFetchAbsent(HelixPropertyStore<ZNRecord> store) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(TABLE);
    when(store.get(eq(path), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(null);
  }

  private static void stubSet(HelixPropertyStore<ZNRecord> store, boolean result) {
    when(store.set(anyString(), any(ZNRecord.class), anyInt(), eq(AccessOption.PERSISTENT)))
        .thenReturn(result);
  }

  private static MaterializedViewRuntimeMetadata capturePersisted(
      HelixPropertyStore<ZNRecord> store, int expectedVersion) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(TABLE);
    ArgumentCaptor<ZNRecord> captor = ArgumentCaptor.forClass(ZNRecord.class);
    verify(store).set(eq(path), captor.capture(), eq(expectedVersion), eq(AccessOption.PERSISTENT));
    return MaterializedViewRuntimeMetadata.fromZNRecord(captor.getValue());
  }
}

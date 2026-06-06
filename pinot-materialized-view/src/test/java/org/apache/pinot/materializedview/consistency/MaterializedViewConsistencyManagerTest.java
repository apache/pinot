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
package org.apache.pinot.materializedview.consistency;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadata;
import org.apache.pinot.materializedview.metadata.PartitionFingerprint;
import org.apache.pinot.materializedview.metadata.PartitionInfo;
import org.apache.pinot.materializedview.metadata.PartitionState;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class MaterializedViewConsistencyManagerTest {
  private static final String BASE_TABLE = "baseTable";
  private static final String MV_TABLE = "mvTable_OFFLINE";
  private static final long BUCKET_MS = 86_400_000L;

  @Test
  public void testEpochZeroRangeMarksPartitionStale()
      throws Exception {
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    String runtimePath = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(MV_TABLE);
    MaterializedViewRuntimeMetadata runtime = new MaterializedViewRuntimeMetadata(
        MV_TABLE, 2 * BUCKET_MS,
        Map.of(
            0L, validInfo(),
            BUCKET_MS, validInfo()));
    when(propertyStore.get(eq(runtimePath), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(runtime.toZNRecord());
    when(propertyStore.set(eq(runtimePath), any(ZNRecord.class), eq(0), eq(AccessOption.PERSISTENT)))
        .thenReturn(true);

    MaterializedViewConsistencyManager manager = new MaterializedViewConsistencyManager();
    manager.init(propertyStore);
    manager.onMaterializedViewTableCreated(MV_TABLE, List.of(BASE_TABLE));
    manager.onBaseTableDataChange(BASE_TABLE, 0L, BUCKET_MS - 1);

    manager.flush(BASE_TABLE);
    manager.stop();

    ArgumentCaptor<ZNRecord> recordCaptor = ArgumentCaptor.forClass(ZNRecord.class);
    verify(propertyStore).set(eq(runtimePath), recordCaptor.capture(), eq(0), eq(AccessOption.PERSISTENT));
    MaterializedViewRuntimeMetadata updated =
        MaterializedViewRuntimeMetadata.fromZNRecord(recordCaptor.getValue());
    assertEquals(updated.getPartitions().get(0L).getState(), PartitionState.STALE);
    assertEquals(updated.getPartitions().get(BUCKET_MS).getState(), PartitionState.VALID);
  }

  @Test
  public void testDefinitionCreatedAfterInitRegistersBaseTableMapping()
      throws Exception {
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    String definitionParentPath = ZKMetadataProvider.getPropertyStorePathForMaterializedViewDefinitionPrefix();
    String definitionPath = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewDefinition(MV_TABLE);
    String runtimePath = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(MV_TABLE);

    when(propertyStore.getChildNames(eq(definitionParentPath), eq(AccessOption.PERSISTENT)))
        .thenReturn(List.of(), List.of(), List.of(MV_TABLE));
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        MV_TABLE, List.of(BASE_TABLE), "SELECT count(*) FROM baseTable", Map.of(), null);
    when(propertyStore.get(eq(definitionPath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(definition.toZNRecord());
    // rebuildReverseIndex verifies the MV's TableConfig still exists and is an MV; mock a
    // matching znode so the orphan-skip check doesn't drop this MV.
    String tableConfigPath = ZKMetadataProvider.constructPropertyStorePathForResourceConfig(MV_TABLE);
    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MV_TABLE).setIsMaterializedView(true).build();
    when(propertyStore.get(eq(tableConfigPath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(TableConfigSerDeUtils.toZNRecord(mvTableConfig));

    MaterializedViewRuntimeMetadata runtime = new MaterializedViewRuntimeMetadata(
        MV_TABLE, 2 * BUCKET_MS,
        Map.of(
            0L, validInfo(),
            BUCKET_MS, validInfo()));
    when(propertyStore.get(eq(runtimePath), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(runtime.toZNRecord());
    when(propertyStore.set(eq(runtimePath), any(ZNRecord.class), eq(0), eq(AccessOption.PERSISTENT)))
        .thenReturn(true);

    MaterializedViewConsistencyManager manager = new MaterializedViewConsistencyManager();
    manager.init(propertyStore);

    ArgumentCaptor<IZkChildListener> childListenerCaptor = ArgumentCaptor.forClass(IZkChildListener.class);
    verify(propertyStore).subscribeChildChanges(eq(definitionParentPath), childListenerCaptor.capture());
    childListenerCaptor.getValue().handleChildChange(definitionParentPath, List.of(MV_TABLE));

    manager.onBaseTableDataChange(BASE_TABLE, 0L, BUCKET_MS - 1);
    manager.flush(BASE_TABLE);
    manager.stop();

    ArgumentCaptor<ZNRecord> recordCaptor = ArgumentCaptor.forClass(ZNRecord.class);
    verify(propertyStore).set(eq(runtimePath), recordCaptor.capture(), eq(0), eq(AccessOption.PERSISTENT));
    MaterializedViewRuntimeMetadata updated =
        MaterializedViewRuntimeMetadata.fromZNRecord(recordCaptor.getValue());
    assertEquals(updated.getPartitions().get(0L).getState(), PartitionState.STALE);
    assertEquals(updated.getPartitions().get(BUCKET_MS).getState(), PartitionState.VALID);
  }

  /// Regression: a stale definition znode whose TableConfig was already removed (best-effort
  /// delete failed mid-DROP) must NOT be re-registered into the reverse index — the rebuild
  /// path must skip the orphan so DROP-followed-by-CREATE doesn't resurrect a ghost MV.
  @Test
  public void testOrphanDefinitionZnodeWithoutTableConfigIsSkipped()
      throws Exception {
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    String definitionParentPath = ZKMetadataProvider.getPropertyStorePathForMaterializedViewDefinitionPrefix();
    String definitionPath = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewDefinition(MV_TABLE);
    String tableConfigPath = ZKMetadataProvider.constructPropertyStorePathForResourceConfig(MV_TABLE);
    String runtimePath = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(MV_TABLE);

    when(propertyStore.getChildNames(eq(definitionParentPath), eq(AccessOption.PERSISTENT)))
        .thenReturn(List.of(MV_TABLE));
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        MV_TABLE, List.of(BASE_TABLE), "SELECT count(*) FROM baseTable", Map.of(), null);
    when(propertyStore.get(eq(definitionPath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(definition.toZNRecord());
    // TableConfig is gone — the prior DROP succeeded at removing the config but failed the
    // best-effort znode cleanup, leaving the definition orphaned.
    when(propertyStore.get(eq(tableConfigPath), any(), eq(AccessOption.PERSISTENT))).thenReturn(null);

    MaterializedViewConsistencyManager manager = new MaterializedViewConsistencyManager();
    manager.init(propertyStore);
    // Orphan was skipped, so the reverse index has no mapping for BASE_TABLE; an event for
    // BASE_TABLE must produce no runtime znode writes.
    manager.onBaseTableDataChange(BASE_TABLE, 0L, BUCKET_MS - 1);
    manager.flush(BASE_TABLE);
    manager.stop();
    verify(propertyStore, never())
        .set(eq(runtimePath), any(ZNRecord.class), eq(0), eq(AccessOption.PERSISTENT));
  }

  /// Regression test for M1: full-range invalidation must NOT create synthetic STALE entries
  /// for buckets that are not present in the partition map. Under Design C, absent buckets
  /// mean "MV does not cover this range"; the broker routes those queries to the base.
  @Test
  public void testFullInvalidationDoesNotSynthesizeAbsentBuckets()
      throws Exception {
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    String runtimePath = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(MV_TABLE);
    // Watermark covers 10 buckets but only two adjacent buckets (#5, #6) are materialized.
    // Adjacent so `inferBucketMsFromPartitions` derives BUCKET_MS from the gap. Pre-fix, the
    // full-range invalidation would have synthesized 8 STALE entries for absent buckets;
    // post-fix we expect just the two real buckets to flip to STALE.
    MaterializedViewRuntimeMetadata runtime = new MaterializedViewRuntimeMetadata(
        MV_TABLE, 10 * BUCKET_MS,
        Map.of(5 * BUCKET_MS, validInfo(),
            6 * BUCKET_MS, validInfo()));
    when(propertyStore.get(eq(runtimePath), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(runtime.toZNRecord());
    when(propertyStore.set(eq(runtimePath), any(ZNRecord.class), eq(0), eq(AccessOption.PERSISTENT)))
        .thenReturn(true);

    MaterializedViewConsistencyManager manager = new MaterializedViewConsistencyManager();
    manager.init(propertyStore);
    manager.onMaterializedViewTableCreated(MV_TABLE, List.of(BASE_TABLE));
    manager.onBaseTableFullInvalidation(BASE_TABLE);

    manager.flush(BASE_TABLE);
    manager.stop();

    ArgumentCaptor<ZNRecord> recordCaptor = ArgumentCaptor.forClass(ZNRecord.class);
    verify(propertyStore).set(eq(runtimePath), recordCaptor.capture(), eq(0), eq(AccessOption.PERSISTENT));
    MaterializedViewRuntimeMetadata updated =
        MaterializedViewRuntimeMetadata.fromZNRecord(recordCaptor.getValue());
    assertEquals(updated.getPartitions().size(), 2,
        "Full invalidation must not synthesize absent-bucket STALE entries");
    assertEquals(updated.getPartitions().get(5 * BUCKET_MS).getState(), PartitionState.STALE);
    assertEquals(updated.getPartitions().get(6 * BUCKET_MS).getState(), PartitionState.STALE);
  }

  /// Regression test for M3 + the typed-exception narrowing on persist(): a CAS conflict
  /// during STALE marking is silently retried; the retry succeeds.
  @Test
  public void testCasConflictTriggersRetry()
      throws Exception {
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    String runtimePath = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(MV_TABLE);
    MaterializedViewRuntimeMetadata runtime = new MaterializedViewRuntimeMetadata(
        MV_TABLE, 2 * BUCKET_MS,
        Map.of(0L, validInfo(), BUCKET_MS, validInfo()));
    when(propertyStore.get(eq(runtimePath), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(runtime.toZNRecord());
    // First write fails (CAS conflict), second succeeds. Retry must converge.
    when(propertyStore.set(eq(runtimePath), any(ZNRecord.class), eq(0), eq(AccessOption.PERSISTENT)))
        .thenReturn(false, true);

    MaterializedViewConsistencyManager manager = new MaterializedViewConsistencyManager();
    manager.init(propertyStore);
    manager.onMaterializedViewTableCreated(MV_TABLE, List.of(BASE_TABLE));
    manager.onBaseTableDataChange(BASE_TABLE, 0L, BUCKET_MS - 1);

    manager.flush(BASE_TABLE);
    manager.stop();

    // Two set() invocations expected — first returned false (CAS conflict), second succeeded.
    verify(propertyStore, org.mockito.Mockito.times(2))
        .set(eq(runtimePath), any(ZNRecord.class), eq(0), eq(AccessOption.PERSISTENT));
  }

  /// Regression test for the lock-free fast-path on onBaseTableDataChange when no MV
  /// references the base table — must return without touching the property store.
  @Test
  public void testNoDependentMvSkipsPropertyStore()
      throws Exception {
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);

    MaterializedViewConsistencyManager manager = new MaterializedViewConsistencyManager();
    manager.init(propertyStore);
    // No onMaterializedViewTableCreated call; reverse index is empty.
    manager.onBaseTableDataChange(BASE_TABLE, 0L, 1_000_000L);
    manager.flush(BASE_TABLE);
    manager.stop();

    // The property-store `set` MUST never be invoked when no MV depends on the base table.
    verify(propertyStore, org.mockito.Mockito.never())
        .set(org.mockito.ArgumentMatchers.anyString(), any(ZNRecord.class),
            org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt());
  }

  // ---------------------------------------------------------------------------
  //  findStrandedEmptyBuckets — the periodic VALID-empty re-evaluation sweep's pure decision
  //  core: which in-coverage VALID-empty buckets have a source window that regained segments and
  //  must therefore be re-marked STALE (closing the residual DELETE-vs-backfill race).
  // ---------------------------------------------------------------------------

  @Test
  public void testFindStrandedEmptyBucketsReMarksWhenSourceRegainedSegments() {
    // W0 is VALID-empty but its source window now has a segment (a backfill that the DELETE commit
    // guard raced) => stranded => must be re-marked STALE.  W1 (VALID, non-empty) and W2 (STALE)
    // are not VALID-empty, so they are never considered.
    Map<Long, PartitionInfo> partitions = Map.of(
        0L, validEmptyInfo(),
        BUCKET_MS, validInfo(),
        2 * BUCKET_MS, staleInfo());
    List<SegmentZKMetadata> sourceSegments = List.of(segment("s0", 0L, BUCKET_MS - 1, 11L));

    List<Long> stranded = MaterializedViewConsistencyManager.findStrandedEmptyBuckets(
        partitions, 3 * BUCKET_MS, sourceSegments, BUCKET_MS);

    assertEquals(stranded, List.of(0L));
  }

  @Test
  public void testFindStrandedEmptyBucketsLeavesEmptyWhenSourceStillEmpty() {
    // W0 is VALID-empty and its source window is still empty (the legitimate DELETE / empty-APPEND
    // outcome) => must NOT be re-marked (re-marking would churn a no-op DELETE every sweep).
    Map<Long, PartitionInfo> partitions = Map.of(0L, validEmptyInfo());
    // A segment that exists but does not overlap W0's window [0, BUCKET_MS).
    List<SegmentZKMetadata> sourceSegments =
        List.of(segment("s2", 2 * BUCKET_MS, 3 * BUCKET_MS, 22L));

    List<Long> stranded = MaterializedViewConsistencyManager.findStrandedEmptyBuckets(
        partitions, BUCKET_MS, sourceSegments, BUCKET_MS);

    assertTrue(stranded.isEmpty(), "VALID-empty bucket with still-empty source must be left STALE-free");
  }

  @Test
  public void testFindStrandedEmptyBucketsIgnoresNonEmptyValidAndStaleBuckets() {
    // W0 is VALID but non-empty (segmentCount=1) — not a VALID-empty bucket.  W1 is STALE — already
    // queued for the scheduler.  Even with source segments overlapping both, neither is returned.
    Map<Long, PartitionInfo> partitions = Map.of(
        0L, validInfo(),
        BUCKET_MS, staleInfo());
    List<SegmentZKMetadata> sourceSegments = List.of(segment("s", 0L, 2 * BUCKET_MS, 33L));

    List<Long> stranded = MaterializedViewConsistencyManager.findStrandedEmptyBuckets(
        partitions, 3 * BUCKET_MS, sourceSegments, BUCKET_MS);

    assertTrue(stranded.isEmpty(), "Only VALID-empty buckets are eligible for the sweep");
  }

  @Test
  public void testFindStrandedEmptyBucketsIgnoresBucketsAtOrPastWatermark() {
    // A VALID-empty bucket whose start is not strictly below the watermark is the not-yet-covered
    // frontier, not an in-coverage empty — it must be ignored even if its source has segments.
    Map<Long, PartitionInfo> partitions = Map.of(BUCKET_MS, validEmptyInfo());
    List<SegmentZKMetadata> sourceSegments =
        List.of(segment("s1", BUCKET_MS, 2 * BUCKET_MS - 1, 44L));

    List<Long> stranded = MaterializedViewConsistencyManager.findStrandedEmptyBuckets(
        partitions, BUCKET_MS, sourceSegments, BUCKET_MS);

    assertTrue(stranded.isEmpty(), "Buckets at/past the watermark are not in-coverage");
  }

  /// End-to-end orchestration: the per-MV sweep reads the runtime + source segments and, finding a
  /// VALID-empty bucket whose source window regained segments, re-marks exactly that bucket STALE
  /// (leaving other buckets untouched) via the partition manager's CAS write.
  @Test
  public void testSweepForViewReMarksStrandedEmptyBucketStale()
      throws Exception {
    String baseTableOffline = TableNameBuilder.OFFLINE.tableNameWithType(BASE_TABLE);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    String runtimePath = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(MV_TABLE);

    // Runtime: W0 is VALID-empty (a DELETE/empty-APPEND outcome), W1 is VALID non-empty. Both are
    // in coverage (watermark = 2 * BUCKET_MS).
    MaterializedViewRuntimeMetadata runtime = new MaterializedViewRuntimeMetadata(
        MV_TABLE, 2 * BUCKET_MS,
        Map.of(0L, validEmptyInfo(), BUCKET_MS, validInfo()));
    when(propertyStore.get(eq(runtimePath), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(runtime.toZNRecord());
    when(propertyStore.set(eq(runtimePath), any(ZNRecord.class), eq(0), eq(AccessOption.PERSISTENT)))
        .thenReturn(true);

    // MV table config supplies bucketMs (= 1d = BUCKET_MS) to inferBucketMs.
    String mvConfigPath = ZKMetadataProvider.constructPropertyStorePathForResourceConfig(MV_TABLE);
    TableTaskConfig taskConfig = new TableTaskConfig(Map.of(CommonConstants.MaterializedViewTask.TASK_TYPE,
        Map.of(CommonConstants.MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d")));
    TableConfig mvConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MV_TABLE).setIsMaterializedView(true).setTaskConfig(taskConfig).build();
    when(propertyStore.get(eq(mvConfigPath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(TableConfigSerDeUtils.toZNRecord(mvConfig));

    // Source base table resolves as OFFLINE.
    String baseConfigPath = ZKMetadataProvider.constructPropertyStorePathForResourceConfig(baseTableOffline);
    TableConfig baseConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(BASE_TABLE).build();
    when(propertyStore.get(eq(baseConfigPath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(TableConfigSerDeUtils.toZNRecord(baseConfig));

    // Source segments: one overlapping W0's window [0, BUCKET_MS) — i.e. the backfill that strands
    // the VALID-empty bucket.
    String segmentsParentPath = ZKMetadataProvider.constructPropertyStorePathForResource(baseTableOffline);
    when(propertyStore.getChildren(eq(segmentsParentPath), any(), eq(AccessOption.PERSISTENT), anyInt(), anyInt()))
        .thenReturn(List.of(segment("s0", 0L, BUCKET_MS - 1, 11L).toZNRecord()));

    MaterializedViewConsistencyManager manager = new MaterializedViewConsistencyManager();
    manager.init(propertyStore);
    manager.sweepStrandedEmptyPartitionsForView(MV_TABLE, BASE_TABLE);
    manager.stop();

    ArgumentCaptor<ZNRecord> recordCaptor = ArgumentCaptor.forClass(ZNRecord.class);
    verify(propertyStore).set(eq(runtimePath), recordCaptor.capture(), eq(0), eq(AccessOption.PERSISTENT));
    MaterializedViewRuntimeMetadata updated =
        MaterializedViewRuntimeMetadata.fromZNRecord(recordCaptor.getValue());
    assertEquals(updated.getPartitions().get(0L).getState(), PartitionState.STALE,
        "Stranded VALID-empty bucket must be re-marked STALE");
    assertEquals(updated.getPartitions().get(BUCKET_MS).getState(), PartitionState.VALID,
        "Non-empty VALID bucket must be left untouched");
  }

  /// The all-MV sweep isolates per-MV failures: one MV whose runtime read throws must not abort
  /// the pass before the others are processed.
  @Test
  public void testSweepIsolatesPerMvFailures()
      throws Exception {
    String mvA = "mvA_OFFLINE";
    String mvB = "mvB_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    String runtimePathA = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(mvA);
    String runtimePathB = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(mvB);

    // MV A's runtime read throws — its per-MV sweep must fail in isolation.
    when(propertyStore.get(eq(runtimePathA), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenThrow(new RuntimeException("boom"));
    // MV B is clean (no in-coverage VALID-empty bucket), so it short-circuits after the runtime read.
    MaterializedViewRuntimeMetadata runtimeB = new MaterializedViewRuntimeMetadata(
        mvB, 2 * BUCKET_MS, Map.of(0L, validInfo()));
    when(propertyStore.get(eq(runtimePathB), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(runtimeB.toZNRecord());

    MaterializedViewConsistencyManager manager = new MaterializedViewConsistencyManager();
    manager.init(propertyStore);
    manager.onMaterializedViewTableCreated(mvA, List.of("baseA"));
    manager.onMaterializedViewTableCreated(mvB, List.of("baseB"));

    // Must not throw despite MV A failing mid-pass.
    manager.sweepStrandedEmptyPartitions();
    manager.stop();

    // Both MVs were attempted — MV A's failure did not abort the pass before MV B was processed.
    verify(propertyStore).get(eq(runtimePathA), any(Stat.class), eq(AccessOption.PERSISTENT));
    verify(propertyStore).get(eq(runtimePathB), any(Stat.class), eq(AccessOption.PERSISTENT));
    // The clean MV B has no stranded bucket, so no STALE write is issued for it.
    verify(propertyStore, never())
        .set(eq(runtimePathB), any(ZNRecord.class), anyInt(), eq(AccessOption.PERSISTENT));
  }

  private static PartitionInfo validInfo() {
    return PartitionInfo.forTesting(PartitionState.VALID, new PartitionFingerprint(1, 1234L), 10L);
  }

  private static PartitionInfo validEmptyInfo() {
    return PartitionInfo.forTesting(PartitionState.VALID, PartitionFingerprint.EMPTY, 10L);
  }

  private static PartitionInfo staleInfo() {
    return PartitionInfo.forTesting(PartitionState.STALE, PartitionFingerprint.EMPTY, 10L);
  }

  private static SegmentZKMetadata segment(String name, long startMs, long endMs, long crc) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(name);
    segmentZKMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    segmentZKMetadata.setStartTime(startMs);
    segmentZKMetadata.setEndTime(endMs);
    segmentZKMetadata.setCrc(crc);
    return segmentZKMetadata;
  }
}

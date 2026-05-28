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
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadata;
import org.apache.pinot.materializedview.metadata.PartitionFingerprint;
import org.apache.pinot.materializedview.metadata.PartitionInfo;
import org.apache.pinot.materializedview.metadata.PartitionState;
import org.apache.zookeeper.data.Stat;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


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

  private static PartitionInfo validInfo() {
    return new PartitionInfo(PartitionState.VALID, new PartitionFingerprint(1, 1234L), 10L);
  }
}

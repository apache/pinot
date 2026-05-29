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
package org.apache.pinot.controller.helix.core;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.materializedview.consistency.MaterializedViewConsistencyManager;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadataUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Unit tests for the {@code MaterializedViewConsistencyManager} reverse-index backfill on
/// {@link PinotHelixResourceManager#backfillMaterializedViewReverseIndex} and the symmetric
/// DROP-MV `extractSourceTableName` fallback inside
/// {@code notifyMaterializedViewConsistencyManagerForTableDrop}.
///
/// Together these close the post-restart orphan window xiangfu0 flagged in #18544: an MV
/// whose definition znode is missing (best-effort persist failed at create, znode removed by
/// manual ZK surgery, or pre-existing from a controller version before definition znodes
/// existed) was previously invisible to the in-memory reverse index after restart, so the
/// `DROP TABLE` delete-guard would let an operator silently orphan the MV by dropping its
/// base. The contract under test is:
///
///   1. Backfill registers every authoritative-MV (`tableConfig.isMaterializedView()=true`)
///      whose definition znode is missing, using the same `extractSourceTableName` fallback
///      as the in-session create path so post-restart and same-session reverse indexes are
///      byte-identical.
///   2. Backfill is fail-isolated per MV — one corrupt `definedSQL` does not break startup
///      for the rest of the cluster's MVs.
///   3. Backfill skips MVs that already have a znode (the manager's `init()` rebuilt those)
///      and never double-registers.
///   4. The DROP path's znode-missing branch falls back to the same `extractSourceTableName`
///      mechanism so an MV whose znode persist failed at create still cleans up its in-memory
///      reverse-index entry on drop, instead of leaking a ghost that blocks legitimate
///      `DROP TABLE` on its base.
///
/// Helix wiring is mocked: a partial mock of `PinotHelixResourceManager` selectively calls
/// real methods, `_propertyStore` is reflection-injected, `ZKMetadataProvider` and
/// `MaterializedViewDefinitionMetadataUtils` are static-mocked. The
/// `MaterializedViewConsistencyManager` is real (so we observe the in-memory reverse index
/// directly via `getDependentMaterializedViews`).
public class PinotHelixResourceManagerMaterializedViewBackfillTest {

  private static final String SOURCE_RAW = "orders";
  private static final String DEFINED_SQL = "SELECT a FROM " + SOURCE_RAW;

  // ── backfill ────────────────────────────────────────────────────────────────────────────

  /// The canonical recovery scenario: an MV whose definition znode never made it to ZK (the
  /// in-session best-effort persist swallowed an exception). After a restart, the manager's
  /// `rebuildReverseIndex` finds nothing for this MV, but backfill must catch it via the
  /// authoritative TableConfig list and the persisted `definedSQL` fallback. Without this,
  /// `DROP TABLE orders` after the restart would silently orphan `mv_OFFLINE`.
  @Test
  public void registersMvWithMissingDefinitionZnode()
      throws Exception {
    String mvWithType = "mv_OFFLINE";
    PinotHelixResourceManager prm = newPartialMock(Collections.singletonList(mvWithType));
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    injectPropertyStore(prm, propertyStore);
    MaterializedViewConsistencyManager mgr = newConsistencyManagerWithEmptyZk();

    TableConfig mvConfig = mvTableConfig(mvWithType, DEFINED_SQL);
    try (MockedStatic<ZKMetadataProvider> zk = Mockito.mockStatic(ZKMetadataProvider.class);
         MockedStatic<MaterializedViewDefinitionMetadataUtils> def =
             Mockito.mockStatic(MaterializedViewDefinitionMetadataUtils.class)) {
      zk.when(() -> ZKMetadataProvider.getTableConfig(propertyStore, mvWithType)).thenReturn(mvConfig);
      // Source TableConfig missing — phase 2's persistMaterializedViewDefinitionMetadataBestEffort
      // bails out internally with a logged WARN. Phase 1's in-memory registration is still
      // expected to succeed (that is the orphan-protection that matters here).
      zk.when(() -> ZKMetadataProvider.getTableConfig(eq(propertyStore), Mockito.argThat(
          arg -> arg != null && arg.startsWith(SOURCE_RAW)))).thenReturn(null);
      def.when(() -> MaterializedViewDefinitionMetadataUtils.fetch(propertyStore, mvWithType))
          .thenReturn(null);

      prm.backfillMaterializedViewReverseIndex(mgr);

      assertEquals(mgr.getDependentMaterializedViews(SOURCE_RAW), Collections.singletonList(mvWithType),
          "Backfill must register the missing-znode MV in the reverse index using the "
              + "definedSQL fallback so DROP TABLE on its base table is blocked.");
    }
  }

  /// MVs whose definition znode is already present must be skipped — the manager's init()
  /// rebuildReverseIndex already covered them, and re-registering would waste a ZK round-trip
  /// per MV (multiplied by every controller restart on every cluster). The check is structural
  /// (znode-existence), not state-based, so a regression that drops the skip would also leak
  /// CPU on the hot startup path.
  @Test
  public void skipsMvWithExistingDefinitionZnode()
      throws Exception {
    String mvWithType = "mv_OFFLINE";
    PinotHelixResourceManager prm = newPartialMock(Collections.singletonList(mvWithType));
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    injectPropertyStore(prm, propertyStore);
    MaterializedViewConsistencyManager mgr = newConsistencyManagerWithEmptyZk();

    TableConfig mvConfig = mvTableConfig(mvWithType, DEFINED_SQL);
    MaterializedViewDefinitionMetadata existingDef = new MaterializedViewDefinitionMetadata(
        mvWithType, Collections.singletonList(SOURCE_RAW), DEFINED_SQL,
        new HashMap<>(), null);
    try (MockedStatic<ZKMetadataProvider> zk = Mockito.mockStatic(ZKMetadataProvider.class);
         MockedStatic<MaterializedViewDefinitionMetadataUtils> def =
             Mockito.mockStatic(MaterializedViewDefinitionMetadataUtils.class)) {
      zk.when(() -> ZKMetadataProvider.getTableConfig(propertyStore, mvWithType)).thenReturn(mvConfig);
      def.when(() -> MaterializedViewDefinitionMetadataUtils.fetch(propertyStore, mvWithType))
          .thenReturn(existingDef);

      prm.backfillMaterializedViewReverseIndex(mgr);

      // Backfill must NOT register: that is the manager-init's job for znode-backed MVs.
      // Critically, no ZK write was attempted either — verifying createIfAbsent is unnecessary
      // because the existing-znode early-return is upstream of phase 2 entirely.
      def.verify(() -> MaterializedViewDefinitionMetadataUtils.createIfAbsent(any(), any()), never());
      assertTrue(mgr.getDependentMaterializedViews(SOURCE_RAW).isEmpty(),
          "Backfill must not double-register an MV whose authoritative definition znode "
              + "already exists; rebuildReverseIndex inside init() owns that entry.");
    }
  }

  /// One corrupt definedSQL must not block startup for the rest of the cluster's MVs. This
  /// is the failure-isolation contract: backfill is best-effort precisely so an unparseable
  /// SQL on a single MV (operator hand-edited the znode, version skew, etc.) cannot wedge
  /// the controller. The healthy MV must still register.
  @Test
  public void corruptDefinedSqlDoesNotBlockOtherMvs()
      throws Exception {
    String healthyMv = "mv_healthy_OFFLINE";
    String brokenMv = "mv_broken_OFFLINE";
    PinotHelixResourceManager prm = newPartialMock(Arrays.asList(healthyMv, brokenMv));
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    injectPropertyStore(prm, propertyStore);
    MaterializedViewConsistencyManager mgr = newConsistencyManagerWithEmptyZk();

    TableConfig healthyConfig = mvTableConfig(healthyMv, DEFINED_SQL);
    TableConfig brokenConfig = mvTableConfig(brokenMv, "this is not valid SQL");
    try (MockedStatic<ZKMetadataProvider> zk = Mockito.mockStatic(ZKMetadataProvider.class);
         MockedStatic<MaterializedViewDefinitionMetadataUtils> def =
             Mockito.mockStatic(MaterializedViewDefinitionMetadataUtils.class)) {
      zk.when(() -> ZKMetadataProvider.getTableConfig(propertyStore, healthyMv)).thenReturn(healthyConfig);
      zk.when(() -> ZKMetadataProvider.getTableConfig(propertyStore, brokenMv)).thenReturn(brokenConfig);
      // Source TableConfig lookup for the healthy MV's source returns null so phase 2 bails
      // out inside the existing best-effort persist — keeps the test focused on phase 1.
      zk.when(() -> ZKMetadataProvider.getTableConfig(eq(propertyStore), Mockito.argThat(
          arg -> arg != null && arg.startsWith(SOURCE_RAW)))).thenReturn(null);
      def.when(() -> MaterializedViewDefinitionMetadataUtils.fetch(eq(propertyStore), any()))
          .thenReturn(null);

      prm.backfillMaterializedViewReverseIndex(mgr);

      assertEquals(mgr.getDependentMaterializedViews(SOURCE_RAW), Collections.singletonList(healthyMv),
          "The healthy MV must register despite a sibling MV's unparseable definedSQL — a "
              + "single corrupt SQL must not wedge the rest of the cluster's reverse index.");
    }
  }

  /// MVs whose `MaterializedViewTask` task config is absent (a malformed TableConfig that
  /// somehow has `isMaterializedView=true` but no task configs) are skipped silently rather
  /// than throwing. This is an unreachable shape under the current DDL but tests the
  /// defensive guard in `resolveBaseTablesForBackfill` — operators applying TableConfig via
  /// raw JSON could produce it, and a NullPointerException at startup is a non-starter.
  @Test
  public void skipsMvWithMissingTaskConfigs()
      throws Exception {
    String mvWithType = "mv_OFFLINE";
    PinotHelixResourceManager prm = newPartialMock(Collections.singletonList(mvWithType));
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    injectPropertyStore(prm, propertyStore);
    MaterializedViewConsistencyManager mgr = newConsistencyManagerWithEmptyZk();

    // Declared as MV but task configs absent — TableConfigBuilder leaves task type map null
    // when no task config is added.
    TableConfig mvConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(mvWithType)
        .setIsMaterializedView(true)
        .build();
    try (MockedStatic<ZKMetadataProvider> zk = Mockito.mockStatic(ZKMetadataProvider.class);
         MockedStatic<MaterializedViewDefinitionMetadataUtils> def =
             Mockito.mockStatic(MaterializedViewDefinitionMetadataUtils.class)) {
      zk.when(() -> ZKMetadataProvider.getTableConfig(propertyStore, mvWithType)).thenReturn(mvConfig);
      def.when(() -> MaterializedViewDefinitionMetadataUtils.fetch(propertyStore, mvWithType))
          .thenReturn(null);

      prm.backfillMaterializedViewReverseIndex(mgr);

      assertTrue(mgr.getDependentMaterializedViews(SOURCE_RAW).isEmpty(),
          "MV with no MaterializedViewTask task configs must be skipped, not throw.");
    }
  }

  /// Empty cluster: backfill must short-circuit cleanly. Tests the fast-path log line so
  /// regressions that turn it into an exception (e.g. a future "let me also write the
  /// listener-debounce config znode here" change) get caught.
  @Test
  public void emptyClusterIsNoOp()
      throws Exception {
    PinotHelixResourceManager prm = newPartialMock(Collections.emptyList());
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    injectPropertyStore(prm, propertyStore);
    MaterializedViewConsistencyManager mgr = newConsistencyManagerWithEmptyZk();

    try (MockedStatic<ZKMetadataProvider> zk = Mockito.mockStatic(ZKMetadataProvider.class);
         MockedStatic<MaterializedViewDefinitionMetadataUtils> def =
             Mockito.mockStatic(MaterializedViewDefinitionMetadataUtils.class)) {
      prm.backfillMaterializedViewReverseIndex(mgr);

      def.verify(() -> MaterializedViewDefinitionMetadataUtils.fetch(any(), any()), never());
      def.verify(() -> MaterializedViewDefinitionMetadataUtils.createIfAbsent(any(), any()), never());
      assertTrue(mgr.getDependentMaterializedViews(SOURCE_RAW).isEmpty());
    }
  }

  /// A null consistency manager argument must be a fast no-op rather than NPE. Defensive
  /// against future refactors that wire backfill into a code path where the manager hasn't
  /// been constructed yet.
  @Test
  public void nullConsistencyManagerIsNoOp()
      throws Exception {
    PinotHelixResourceManager prm = newPartialMock(Collections.singletonList("mv_OFFLINE"));
    prm.backfillMaterializedViewReverseIndex(null);
    // No assertion needed — the test passes iff no NPE is thrown.
  }

  // ── DROP MV symmetric fallback ──────────────────────────────────────────────────────────

  /// The matching half of the backfill: when a DROP MV runs against an MV whose definition
  /// znode is absent (the same failure mode backfill recovers from), the drop path must
  /// also fall back to `extractSourceTableName` so the in-memory reverse-index entry —
  /// whether registered by backfill, by the in-session create-fallback, or by a future code
  /// path — is cleaned up. Without this, dropping a backfilled MV whose phase-2 znode write
  /// also failed would leak a ghost reverse-index entry that subsequently blocks legitimate
  /// `DROP TABLE` on its (now genuinely independent) base table. Asymmetry between create
  /// and drop fallback paths is the bug this test pins.
  @Test
  public void dropFallbackUnregistersViaDefinedSqlWhenZnodeAbsent()
      throws Exception {
    String mvWithType = "mv_OFFLINE";
    PinotHelixResourceManager prm = mock(PinotHelixResourceManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    injectPropertyStore(prm, propertyStore);
    MaterializedViewConsistencyManager mgr = newConsistencyManagerWithEmptyZk();
    injectConsistencyManager(prm, mgr);

    // Pre-state: the MV is in the reverse index pointing at SOURCE_RAW.  This is what
    // backfill leaves behind, or what the in-session fallback at create time leaves behind.
    mgr.onMaterializedViewTableCreated(mvWithType, Collections.singletonList(SOURCE_RAW));
    assertFalse(mgr.getDependentMaterializedViews(SOURCE_RAW).isEmpty(),
        "Pre-condition: reverse index must hold an entry for the MV before drop.");

    TableConfig mvConfig = mvTableConfig(mvWithType, DEFINED_SQL);
    try (MockedStatic<ZKMetadataProvider> zk = Mockito.mockStatic(ZKMetadataProvider.class);
         MockedStatic<MaterializedViewDefinitionMetadataUtils> def =
             Mockito.mockStatic(MaterializedViewDefinitionMetadataUtils.class)) {
      zk.when(() -> ZKMetadataProvider.getTableConfig(propertyStore, mvWithType)).thenReturn(mvConfig);
      // Definition znode absent — the failure mode the new fallback covers.
      def.when(() -> MaterializedViewDefinitionMetadataUtils.fetch(propertyStore, mvWithType))
          .thenReturn(null);

      invokeDropNotify(prm, mvWithType);

      assertTrue(mgr.getDependentMaterializedViews(SOURCE_RAW).isEmpty(),
          "DROP MV with znode missing must fall back to extractSourceTableName(definedSQL) "
              + "and unregister the in-memory entry; otherwise the reverse index leaks a "
              + "ghost dependency on the base table.");
    }
  }

  // ── helpers ─────────────────────────────────────────────────────────────────────────────

  private static TableConfig mvTableConfig(String tableNameWithType, String definedSql) {
    Map<String, Map<String, String>> taskTypeConfigsMap = new HashMap<>();
    Map<String, String> mvTaskConfigs = new HashMap<>();
    mvTaskConfigs.put(CommonConstants.MaterializedViewTask.DEFINED_SQL_KEY, definedSql);
    taskTypeConfigsMap.put(CommonConstants.MaterializedViewTask.TASK_TYPE, mvTaskConfigs);
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableNameWithType)
        .setIsMaterializedView(true)
        .setTaskConfig(new org.apache.pinot.spi.config.table.TableTaskConfig(taskTypeConfigsMap))
        .build();
  }

  /// Returns a partial-mock `PinotHelixResourceManager` whose `getAllResources`/
  /// `getAllRawMaterializedViewNames` and `backfillMaterializedViewReverseIndex` call the
  /// real implementation while leaving everything else mocked. Mirrors the partial-mock
  /// pattern in `PinotHelixResourceManagerMaterializedViewListingTest` — a real construction
  /// of `PinotHelixResourceManager` requires a live cluster and is the wrong layer for this
  /// targeted unit test.
  private static PinotHelixResourceManager newPartialMock(List<String> offlineResources) {
    PinotHelixResourceManager prm = mock(PinotHelixResourceManager.class);
    when(prm.getAllResources()).thenReturn(offlineResources);
    when(prm.getAllRawMaterializedViewNames(Mockito.any())).thenCallRealMethod();
    Mockito.doCallRealMethod().when(prm).backfillMaterializedViewReverseIndex(Mockito.any());
    return prm;
  }

  /// Builds a real `MaterializedViewConsistencyManager` whose `init` reads an empty children
  /// list, so its initial reverse index is empty and tests can observe registrations
  /// happening exclusively from the path under test.
  private static MaterializedViewConsistencyManager newConsistencyManagerWithEmptyZk() {
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> mgrStore = mock(ZkHelixPropertyStore.class);
    String defParent = ZKMetadataProvider.getPropertyStorePathForMaterializedViewDefinitionPrefix();
    when(mgrStore.getChildNames(eq(defParent), eq(AccessOption.PERSISTENT)))
        .thenReturn(Collections.emptyList());
    MaterializedViewConsistencyManager mgr = new MaterializedViewConsistencyManager();
    mgr.init(mgrStore);
    return mgr;
  }

  private static void injectPropertyStore(PinotHelixResourceManager prm,
      ZkHelixPropertyStore<ZNRecord> propertyStore) throws Exception {
    Field field = PinotHelixResourceManager.class.getDeclaredField("_propertyStore");
    field.setAccessible(true);
    field.set(prm, propertyStore);
  }

  private static void injectConsistencyManager(PinotHelixResourceManager prm,
      MaterializedViewConsistencyManager mgr) throws Exception {
    Field field = PinotHelixResourceManager.class.getDeclaredField("_materializedViewConsistencyManager");
    field.setAccessible(true);
    field.set(prm, mgr);
  }

  /// Invokes the private `notifyMaterializedViewConsistencyManagerForTableDrop` via reflection
  /// so the test can target the symmetric fallback branch directly without going through the
  /// full `deleteTable` path (which would require a live Helix cluster). Reflection keeps the
  /// production visibility minimal.
  private static void invokeDropNotify(PinotHelixResourceManager prm, String tableNameWithType)
      throws Exception {
    Method m = PinotHelixResourceManager.class
        .getDeclaredMethod("notifyMaterializedViewConsistencyManagerForTableDrop", String.class);
    m.setAccessible(true);
    m.invoke(prm, tableNameWithType);
  }
}

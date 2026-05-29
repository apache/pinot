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
import java.util.Arrays;
import java.util.List;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Focused unit tests for {@link PinotHelixResourceManager#getAllRawMaterializedViewNames}.
///
/// The filter is a five-line pipeline over `getAllResources()` whose correctness hinges on three
/// contracts: (1) skipping REALTIME resources without a ZK fetch, (2) reading the canonical
/// `TableConfig#isMaterializedView` flag rather than inferring MV-ness from the task block, and
/// (3) honouring the database scope. Each test exercises one of those contracts in isolation
/// so a regression names the broken rule rather than just "listing returned the wrong list".
///
/// Helix wiring is mocked: the test replaces `getAllResources()` on a partial mock and stubs
/// `_propertyStore` via reflection, then static-mocks `ZKMetadataProvider.getTableConfig` so
/// the helper sees deterministic TableConfigs without spinning up a real ZK / Helix stack.
/// Integration coverage for the end-to-end Helix path lives with the rest of the
/// resource-manager integration tests under `helix/core/PinotHelixResourceManager*Test`.
public class PinotHelixResourceManagerMaterializedViewListingTest {

  @Test
  public void filtersByIsMaterializedViewFlagAndSkipsRealtime()
      throws Exception {
    PinotHelixResourceManager prm = mock(PinotHelixResourceManager.class);
    when(prm.getAllRawMaterializedViewNames(Mockito.any())).thenCallRealMethod();
    when(prm.getAllResources()).thenReturn(Arrays.asList(
        "mv_orders_OFFLINE",        // MV → kept
        "plain_orders_OFFLINE",     // OFFLINE but no MV flag → dropped
        "mv_orders_REALTIME",       // REALTIME → dropped (and never fetched)
        "ghost_OFFLINE"));          // TableConfig missing (corrupted znode) → dropped

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    injectPropertyStore(prm, propertyStore);

    TableConfig mvConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders_OFFLINE")
        .setIsMaterializedView(true)
        .build();
    TableConfig plainConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("plain_orders_OFFLINE")
        .build();

    try (MockedStatic<ZKMetadataProvider> staticZk = Mockito.mockStatic(ZKMetadataProvider.class)) {
      staticZk.when(() -> ZKMetadataProvider.getTableConfig(propertyStore, "mv_orders_OFFLINE"))
          .thenReturn(mvConfig);
      staticZk.when(() -> ZKMetadataProvider.getTableConfig(propertyStore, "plain_orders_OFFLINE"))
          .thenReturn(plainConfig);
      // `ghost_OFFLINE`: TableConfig fetch returns null (corruption). The helper must silently
      // drop it rather than throw — a single broken znode must not blank the entire listing.
      staticZk.when(() -> ZKMetadataProvider.getTableConfig(propertyStore, "ghost_OFFLINE"))
          .thenReturn(null);

      List<String> result = prm.getAllRawMaterializedViewNames(null);

      assertEquals(result, java.util.Collections.singletonList("mv_orders"),
          "Only the OFFLINE resource whose TableConfig has isMaterializedView=true must be "
              + "returned, and the raw name must be stripped of its _OFFLINE suffix so the "
              + "result is reusable as input to SHOW CREATE / DROP MATERIALIZED VIEW.");
      // Critical: the REALTIME resource must NEVER trigger a TableConfig fetch — that would
      // be a wasted ZK round-trip on every realtime table in the cluster.
      staticZk.verify(() -> ZKMetadataProvider.getTableConfig(propertyStore, "mv_orders_REALTIME"),
          Mockito.never());
    }
  }

  @Test
  public void scopesListingToProvidedDatabase()
      throws Exception {
    PinotHelixResourceManager prm = mock(PinotHelixResourceManager.class);
    when(prm.getAllRawMaterializedViewNames(Mockito.any())).thenCallRealMethod();
    when(prm.getAllResources()).thenReturn(Arrays.asList(
        "mv_global_OFFLINE",        // default db
        "analytics.mv_revenue_OFFLINE",   // 'analytics' db → kept
        "marketing.mv_funnel_OFFLINE"));  // 'marketing' db → dropped

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    injectPropertyStore(prm, propertyStore);

    TableConfig analyticsMv = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("analytics.mv_revenue_OFFLINE")
        .setIsMaterializedView(true)
        .build();

    try (MockedStatic<ZKMetadataProvider> staticZk = Mockito.mockStatic(ZKMetadataProvider.class)) {
      staticZk.when(() -> ZKMetadataProvider.getTableConfig(propertyStore,
          "analytics.mv_revenue_OFFLINE")).thenReturn(analyticsMv);
      // marketing.mv_funnel is filtered by the database predicate BEFORE we hit ZK, so no stub
      // is needed; if the database filter regresses this test will fail with an
      // `UnnecessaryStubbingException`-equivalent (an unstubbed call returning null) which
      // makes the regression obvious rather than passing silently.

      List<String> result = prm.getAllRawMaterializedViewNames("analytics");

      // The "raw" name strips the type suffix but RETAINS the database prefix — exactly what
      // SHOW TABLES does. Keeping the qualifier means a caller running
      // `SHOW MATERIALIZED VIEWS FROM analytics` from a session with no Database header still
      // sees the fully-qualified name and can pipe it straight into
      // `SHOW CREATE MATERIALIZED VIEW analytics.mv_revenue` without guessing the qualifier.
      assertEquals(result, java.util.Collections.singletonList("analytics.mv_revenue"),
          "Only MVs whose qualified name starts with 'analytics.' must be returned; cross-database "
              + "leaks would let a caller scoped to one database enumerate another database's MVs.");
      // The default-database MV must NOT have been read either — a regression in the database
      // filter that lets default-db resources through would also let a header substitution
      // attack discover MVs the caller cannot read.
      staticZk.verify(() -> ZKMetadataProvider.getTableConfig(propertyStore, "mv_global_OFFLINE"),
          Mockito.never());
      staticZk.verify(() -> ZKMetadataProvider.getTableConfig(propertyStore,
          "marketing.mv_funnel_OFFLINE"), Mockito.never());
    }
  }

  /// A ZK fetch that throws must NOT break the entire listing — a single corrupted znode is
  /// expected during operator investigation of broken state, and SHOW MATERIALIZED VIEWS is
  /// exactly the verb an operator would run to diagnose it.
  @Test
  public void corruptedZnodeDoesNotBreakListing()
      throws Exception {
    PinotHelixResourceManager prm = mock(PinotHelixResourceManager.class);
    when(prm.getAllRawMaterializedViewNames(Mockito.any())).thenCallRealMethod();
    when(prm.getAllResources()).thenReturn(Arrays.asList(
        "mv_healthy_OFFLINE",
        "mv_broken_OFFLINE"));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    injectPropertyStore(prm, propertyStore);

    TableConfig healthy = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_healthy_OFFLINE")
        .setIsMaterializedView(true)
        .build();

    try (MockedStatic<ZKMetadataProvider> staticZk = Mockito.mockStatic(ZKMetadataProvider.class)) {
      staticZk.when(() -> ZKMetadataProvider.getTableConfig(propertyStore, "mv_healthy_OFFLINE"))
          .thenReturn(healthy);
      staticZk.when(() -> ZKMetadataProvider.getTableConfig(propertyStore, "mv_broken_OFFLINE"))
          .thenThrow(new RuntimeException("Simulated ZK read failure on broken znode"));

      List<String> result = prm.getAllRawMaterializedViewNames(null);

      assertEquals(result.size(), 1,
          "The healthy MV must still appear despite a broken sibling — operators need this "
              + "listing to remain usable while diagnosing corruption.");
      assertTrue(result.contains("mv_healthy"));
    }
  }

  /// Sets `PinotHelixResourceManager#_propertyStore` via reflection. Required because the
  /// real Helix-backed constructor would need a live cluster to populate the field, and the
  /// helper under test reads `_propertyStore` to look up TableConfigs. Using reflection keeps
  /// the test focused on the filter pipeline rather than the manager's construction story.
  private static void injectPropertyStore(PinotHelixResourceManager prm,
      ZkHelixPropertyStore<ZNRecord> propertyStore) throws Exception {
    Field field = PinotHelixResourceManager.class.getDeclaredField("_propertyStore");
    field.setAccessible(true);
    field.set(prm, propertyStore);
  }
}

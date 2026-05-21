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
package org.apache.pinot.materializedview.rewrite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadata;
import org.mockito.invocation.InvocationOnMock;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Regression coverage for the OFFLINE→OFFLINE→ONLINE base-table lifecycle that the M1/F2 fix
/// addresses.  An MV referenced by a base table whose broker resource cycles must remain in (or
/// be restored to) the broker's metadata cache after the cycle — without this, a transient
/// rebalance or operator-driven OFFLINE/ONLINE bounce would permanently silence MV rewrite on
/// this broker until the MV-definition znode is republished.
public class MaterializedViewMetadataCacheLifecycleTest {

  private static final String MV_OFFLINE = "mv_orders_OFFLINE";
  private static final String DEF_PARENT =
      ZKMetadataProvider.getPropertyStorePathForMaterializedViewDefinitionPrefix();
  private static final String RUNTIME_PARENT =
      ZKMetadataProvider.getPropertyStorePathForMaterializedViewRuntimePrefix();
  private static final String DEF_PATH = DEF_PARENT + "/" + MV_OFFLINE;
  private static final String RUNTIME_PATH = RUNTIME_PARENT + "/" + MV_OFFLINE;

  @Test
  public void testBaseTableLifecycleRestoresMaterializedView() {
    // Stub a ZK property store backed by an in-memory map.  Just enough surface to drive the
    // cache through the listener+invalidate+refresh path the broker resource state model uses.
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    Map<String, ZNRecord> store = new ConcurrentHashMap<>();

    ZNRecord defRecord = buildDefinition().toZNRecord();
    ZNRecord runtimeRecord = buildRuntime().toZNRecord();
    store.put(DEF_PATH, defRecord);
    store.put(RUNTIME_PATH, runtimeRecord);

    stubPropertyStore(propertyStore, store);

    MaterializedViewMetadataCache cache = new MaterializedViewMetadataCache(propertyStore);

    // Precondition: cache is hot — the MV is queryable via the base-table reverse index.
    assertNotNull(cache.getMaterializedViewEntriesForBaseTable("orders"),
        "Precondition: cache must hold the MV indexed against its base table 'orders'");
    assertTrue(!cache.getMaterializedViewEntriesForBaseTable("orders").isEmpty(),
        "Precondition: at least one MV entry should reference 'orders'");

    // Simulate the broker resource state model firing ONLINE→OFFLINE on the BASE table.
    cache.invalidateBaseTable("orders");
    assertNull(cache.getMaterializedViewEntriesForBaseTable("orders"),
        "After invalidate, the reverse index must drop the entry");

    // Simulate the matching OFFLINE→ONLINE transition on the BASE table.  Without the F2 fix,
    // this would be a no-op (refreshTable used to look up "<rawTable>_OFFLINE" as an MV name,
    // which fails when rawTable is a base — not an MV).  With the fix, refreshTable walks
    // every MV definition znode and rebuilds entries whose base-table list mentions the
    // transitioning table.
    cache.refreshTable("orders");

    // Postcondition: the MV is back in the cache and the reverse index is restored.
    List<MaterializedViewMetadataCache.MaterializedViewCacheEntry> restored =
        cache.getMaterializedViewEntriesForBaseTable("orders");
    assertNotNull(restored,
        "After refresh, the broker must consider the MV again — F2 base-table-cycle bug");
    assertTrue(restored.stream().anyMatch(e -> e.getMaterializedViewTableNameWithType().equals(MV_OFFLINE)),
        "Restored cache must contain " + MV_OFFLINE + " but got " + restored);
  }

  @Test
  public void testMaterializedViewTableLifecycleRestoresEntry() {
    // The symmetric case: the MV TABLE itself cycles ONLINE→OFFLINE→ONLINE (not the base).
    // refreshTable's "case 1" path (directly look up the MV's own def znode) must rehydrate.
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    Map<String, ZNRecord> store = new ConcurrentHashMap<>();
    store.put(DEF_PATH, buildDefinition().toZNRecord());
    store.put(RUNTIME_PATH, buildRuntime().toZNRecord());

    stubPropertyStore(propertyStore, store);

    MaterializedViewMetadataCache cache = new MaterializedViewMetadataCache(propertyStore);

    // The state model fires invalidate for the MV table's transitioning rawName ("mv_orders").
    cache.invalidateBaseTable("mv_orders");
    assertNull(cache.getMaterializedViewEntriesForBaseTable("orders"),
        "Invalidate with the MV's own raw name must remove the entry (matches the MV's own offline name)");

    // Then ONLINE: refreshTable looks up `mv_orders_OFFLINE` directly (case 1).
    cache.refreshTable("mv_orders");
    List<MaterializedViewMetadataCache.MaterializedViewCacheEntry> restored =
        cache.getMaterializedViewEntriesForBaseTable("orders");
    assertNotNull(restored, "MV-table cycle must also restore the cache entry");
    assertTrue(restored.stream().anyMatch(e -> e.getMaterializedViewTableNameWithType().equals(MV_OFFLINE)));
  }

  private static void stubPropertyStore(ZkHelixPropertyStore<ZNRecord> propertyStore, Map<String, ZNRecord> store) {
    when(propertyStore.getChildNames(anyString(), anyInt())).thenAnswer(invocation -> {
      String parent = invocation.getArgument(0);
      List<String> children = new ArrayList<>();
      for (String path : store.keySet()) {
        if (path.startsWith(parent + "/")) {
          children.add(path.substring(parent.length() + 1));
        }
      }
      return children;
    });
    when(propertyStore.exists(anyString(), anyInt())).thenAnswer(invocation -> {
      String path = invocation.getArgument(0);
      return store.containsKey(path);
    });
    // Two `get` overloads: scalar (String path) and bulk (List<String> paths).  Stub both
    // explicitly so the mocked instance returns real ZNRecords rather than Mockito defaults.
    when(propertyStore.get(any(List.class), any(), anyInt())).thenAnswer((InvocationOnMock invocation) -> {
      @SuppressWarnings("unchecked")
      List<String> paths = (List<String>) invocation.getArgument(0);
      List<ZNRecord> result = new ArrayList<>(paths.size());
      for (String path : paths) {
        result.add(store.get(path));
      }
      return result;
    });
    when(propertyStore.get(anyString(), any(), anyInt())).thenAnswer(invocation -> {
      String path = invocation.getArgument(0);
      return store.get(path);
    });
    // Listener subscriptions: subscribeChildChanges returns List<String> (the initial child list),
    // others are void.  Returning null/empty is fine because the test drives invalidate/refresh
    // directly rather than via fired listener callbacks.
    when(propertyStore.subscribeChildChanges(anyString(), any(IZkChildListener.class))).thenReturn(null);
    doAnswer(invocation -> null).when(propertyStore).subscribeDataChanges(anyString(), any(IZkDataListener.class));
    doAnswer(invocation -> null).when(propertyStore)
        .unsubscribeDataChanges(anyString(), any(IZkDataListener.class));
  }

  private static MaterializedViewDefinitionMetadata buildDefinition() {
    // baseTables stores the RAW base-table name (the controller-side convention; see
    // MaterializedViewClusterIntegrationTest, which constructs definitions with
    // Collections.singletonList(SOURCE_TABLE_NAME)).
    Set<String> baseTables = new HashSet<>();
    baseTables.add("orders");
    return new MaterializedViewDefinitionMetadata(
        MV_OFFLINE,
        new ArrayList<>(baseTables),
        "SELECT ts, SUM(revenue) AS sum_revenue FROM orders GROUP BY ts",
        new HashMap<>(),
        null);
  }

  private static MaterializedViewRuntimeMetadata buildRuntime() {
    return new MaterializedViewRuntimeMetadata(MV_OFFLINE, 1_700_000_000_000L, Collections.emptyMap());
  }
}

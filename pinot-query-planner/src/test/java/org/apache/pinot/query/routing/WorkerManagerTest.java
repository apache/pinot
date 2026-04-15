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
package org.apache.pinot.query.routing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link WorkerManager}.
 */
public class WorkerManagerTest {

  private static Schema.SchemaBuilder getSchemaBuilder(String schemaName) {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("col2", FieldSpec.DataType.STRING, "")
        .addMetric("col3", FieldSpec.DataType.INT, 0)
        .setSchemaName(schemaName);
  }

  private static ServerInstance getServerInstance(String hostname, int port) {
    String server = String.format("%s%s_%d", CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE, hostname, port);
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(server);
    ZNRecord znRecord = instanceConfig.getRecord();
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    simpleFields.put(CommonConstants.Helix.Instance.GRPC_PORT_KEY, String.valueOf(port));
    simpleFields.put(CommonConstants.Helix.Instance.MULTI_STAGE_QUERY_ENGINE_SERVICE_PORT_KEY, String.valueOf(port));
    simpleFields.put(CommonConstants.Helix.Instance.MULTI_STAGE_QUERY_ENGINE_MAILBOX_PORT_KEY, String.valueOf(port));
    return new ServerInstance(instanceConfig);
  }

  /**
   * Tests that when useLeafServerForIntermediateStage is enabled and querying an empty table
   * (which results in no leaf servers), the query planner falls back to using all enabled servers
   * instead of failing.
   *
   * This test simulates the scenario where a table exists with routing but has no segments,
   * resulting in an empty RoutingTable (no server instances with segments).
   */
  @Test
  public void testSingletonWorkerWithEmptyTableAndUseLeafServerEnabled() {
    Schema emptyTableSchema = getSchemaBuilder("emptyTable").build();

    // Create server instances
    ServerInstance server1 = getServerInstance("localhost", 1);
    ServerInstance server2 = getServerInstance("localhost", 2);
    Map<String, ServerInstance> serverInstanceMap = new HashMap<>();
    serverInstanceMap.put(server1.getInstanceId(), server1);
    serverInstanceMap.put(server2.getInstanceId(), server2);

    // Create a routing table with no segments (empty table scenario)
    RoutingTable emptyRoutingTable = new RoutingTable(Collections.emptyMap(), List.of(), 0);

    // Create mock routing manager
    RoutingManager routingManager = new EmptyTableRoutingManager(serverInstanceMap, emptyRoutingTable);

    // Create mock table cache
    Map<String, String> tableNameMap = new HashMap<>();
    tableNameMap.put("emptyTable_OFFLINE", "emptyTable_OFFLINE");
    tableNameMap.put("emptyTable", "emptyTable");

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableNameMap()).thenReturn(tableNameMap);
    when(tableCache.getActualTableName(anyString())).thenAnswer(inv -> tableNameMap.get(inv.getArgument(0)));
    when(tableCache.getSchema(anyString())).thenReturn(emptyTableSchema);
    when(tableCache.getTableConfig("emptyTable_OFFLINE"))
        .thenReturn(mock(org.apache.pinot.spi.config.table.TableConfig.class));

    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, routingManager);
    QueryEnvironment queryEnvironment = new QueryEnvironment(CommonConstants.DEFAULT_DATABASE, tableCache,
        workerManager);

    // This query requires a singleton worker (due to LIMIT) and uses useLeafServerForIntermediateStage
    // When querying an empty table, there are no leaf servers, so we need to fall back to enabled servers
    String query = "SET useLeafServerForIntermediateStage=true; SELECT * FROM emptyTable LIMIT 10";

    // This should not throw "bound must be positive" error anymore
    @SuppressWarnings("deprecation")
    DispatchableSubPlan dispatchableSubPlan = queryEnvironment.planQuery(query);
    assertNotNull(dispatchableSubPlan);
  }

  /**
   * Tests that literal-only stages (e.g. UNION ALL of constant values) are assigned to the same
   * servers as the table-scanning stages, not to all enabled servers across all tenants.
   *
   * <p>Simulates two server tenants: T1 (serves the queried table) and T2 (unrelated). Before the fix,
   * literal-only stages processed before leaf stages would see an empty candidate set and fall back to
   * all enabled servers, potentially landing on T2 servers.
   */
  @Test
  public void testLiteralOnlyStagesUseTableServers() {
    Schema tableSchema = getSchemaBuilder("testTable").build();

    // T1 servers: serve the queried table
    ServerInstance t1Server1 = getServerInstance("t1-host1", 1);
    ServerInstance t1Server2 = getServerInstance("t1-host2", 2);

    // T2 servers: unrelated tenant, should NOT be used
    ServerInstance t2Server1 = getServerInstance("t2-host1", 3);
    ServerInstance t2Server2 = getServerInstance("t2-host2", 4);

    // All enabled servers (both tenants)
    Map<String, ServerInstance> allEnabledServers = new HashMap<>();
    allEnabledServers.put(t1Server1.getInstanceId(), t1Server1);
    allEnabledServers.put(t1Server2.getInstanceId(), t1Server2);
    allEnabledServers.put(t2Server1.getInstanceId(), t2Server1);
    allEnabledServers.put(t2Server2.getInstanceId(), t2Server2);

    // Only T1 servers serve the table
    Set<String> t1ServerIds = Set.of(t1Server1.getInstanceId(), t1Server2.getInstanceId());
    Set<String> t2ServerIds = Set.of(t2Server1.getInstanceId(), t2Server2.getInstanceId());

    // Routing table: T1 servers have segments for testTable
    Map<ServerInstance, SegmentsToQuery> serverSegmentsMap = new HashMap<>();
    serverSegmentsMap.put(t1Server1, new SegmentsToQuery(List.of("seg1", "seg2"), List.of()));
    serverSegmentsMap.put(t1Server2, new SegmentsToQuery(List.of("seg3", "seg4"), List.of()));
    RoutingTable routingTable = new RoutingTable(serverSegmentsMap, List.of(), 0);

    RoutingManager routingManager = new MultiTenantRoutingManager(allEnabledServers, t1ServerIds, routingTable);

    Map<String, String> tableNameMap = new HashMap<>();
    tableNameMap.put("testTable_OFFLINE", "testTable_OFFLINE");
    tableNameMap.put("testTable", "testTable");

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableNameMap()).thenReturn(tableNameMap);
    when(tableCache.getActualTableName(anyString())).thenAnswer(inv -> tableNameMap.get(inv.getArgument(0)));
    when(tableCache.getSchema(anyString())).thenReturn(tableSchema);
    when(tableCache.getTableConfig("testTable_OFFLINE"))
        .thenReturn(mock(org.apache.pinot.spi.config.table.TableConfig.class));

    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 5, routingManager);
    QueryEnvironment queryEnvironment = new QueryEnvironment(CommonConstants.DEFAULT_DATABASE, tableCache,
        workerManager);

    // Query with UNION ALL of literals joined with a table scan. The literal stages (from the subquery)
    // are in a subtree that is traversed before the table scan in post-order.
    String query = "SELECT * FROM ("
        + "  SELECT 1 AS id, 'a' AS val"
        + "  UNION ALL"
        + "  SELECT 2 AS id, 'b' AS val"
        + "  UNION ALL"
        + "  SELECT 3 AS id, 'c' AS val"
        + ") AS literals"
        + " JOIN testTable ON testTable.col1 = literals.val";

    @SuppressWarnings("deprecation")
    DispatchableSubPlan plan = queryEnvironment.planQuery(query);
    assertNotNull(plan);

    // Verify: no stage should be assigned to T2 servers
    Set<String> allAssignedServerIds = new HashSet<>();
    for (Map.Entry<Integer, DispatchablePlanFragment> entry : plan.getQueryStageMap().entrySet()) {
      int stageId = entry.getKey();
      if (stageId == 0) {
        continue; // skip broker root stage
      }
      for (QueryServerInstance server : entry.getValue().getServerInstances()) {
        allAssignedServerIds.add(server.getInstanceId());
      }
    }

    assertFalse(allAssignedServerIds.isEmpty(), "Expected at least one server assignment");
    for (String serverId : allAssignedServerIds) {
      assertFalse(t2ServerIds.contains(serverId),
          "Literal-only stage was incorrectly assigned to T2 server: " + serverId);
    }
    // At least one T1 server should be used (for the table scan)
    boolean anyT1Server = false;
    for (String serverId : allAssignedServerIds) {
      if (t1ServerIds.contains(serverId)) {
        anyT1Server = true;
        break;
      }
    }
    assertTrue(anyT1Server, "Expected at least one T1 server to be used");
  }

  /**
   * A RoutingManager that simulates two tenants of servers, where only one tenant serves the queried
   * tables. {@code getEnabledServerInstanceMap()} returns all servers across both tenants, while
   * {@code getServingInstances()} returns only the tenant's servers.
   */
  private static class MultiTenantRoutingManager implements RoutingManager {
    private final Map<String, ServerInstance> _allEnabledServers;
    private final Set<String> _servingInstanceIds;
    private final RoutingTable _routingTable;

    MultiTenantRoutingManager(Map<String, ServerInstance> allEnabledServers, Set<String> servingInstanceIds,
        RoutingTable routingTable) {
      _allEnabledServers = allEnabledServers;
      _servingInstanceIds = servingInstanceIds;
      _routingTable = routingTable;
    }

    @Override
    public Map<String, ServerInstance> getEnabledServerInstanceMap() {
      return _allEnabledServers;
    }

    @Nullable
    @Override
    public RoutingTable getRoutingTable(BrokerRequest brokerRequest, long requestId) {
      return _routingTable;
    }

    @Nullable
    @Override
    public RoutingTable getRoutingTable(BrokerRequest brokerRequest, String tableNameWithType, long requestId) {
      return _routingTable;
    }

    @Nullable
    @Override
    public List<String> getSegments(BrokerRequest brokerRequest) {
      return new ArrayList<>(_routingTable.getServerInstanceToSegmentsMap().values().iterator().next().getSegments());
    }

    @Override
    public boolean routingExists(String tableNameWithType) {
      return true;
    }

    @Nullable
    @Override
    public TimeBoundaryInfo getTimeBoundaryInfo(String offlineTableName) {
      return null;
    }

    @Nullable
    @Override
    public TablePartitionInfo getTablePartitionInfo(String tableNameWithType) {
      return null;
    }

    @Nullable
    @Override
    public TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo(String tableNameWithType) {
      return null;
    }

    @Override
    public Set<String> getServingInstances(String tableNameWithType) {
      return _servingInstanceIds;
    }

    @Override
    public boolean isTableDisabled(String tableNameWithType) {
      return false;
    }
  }

  /**
   * A custom RoutingManager implementation that simulates a table with routing but no segments.
   * This is used to test the empty leaf server fallback logic.
   */
  private static class EmptyTableRoutingManager implements RoutingManager {
    private final Map<String, ServerInstance> _serverInstanceMap;
    private final RoutingTable _emptyRoutingTable;

    public EmptyTableRoutingManager(Map<String, ServerInstance> serverInstanceMap, RoutingTable emptyRoutingTable) {
      _serverInstanceMap = serverInstanceMap;
      _emptyRoutingTable = emptyRoutingTable;
    }

    @Override
    public Map<String, ServerInstance> getEnabledServerInstanceMap() {
      return _serverInstanceMap;
    }

    @Nullable
    @Override
    public RoutingTable getRoutingTable(BrokerRequest brokerRequest, long requestId) {
      return _emptyRoutingTable;
    }

    @Nullable
    @Override
    public RoutingTable getRoutingTable(BrokerRequest brokerRequest, String tableNameWithType, long requestId) {
      return _emptyRoutingTable;
    }

    @Nullable
    @Override
    public List<String> getSegments(BrokerRequest brokerRequest) {
      return List.of();
    }

    @Override
    public boolean routingExists(String tableNameWithType) {
      return true;
    }

    @Nullable
    @Override
    public TimeBoundaryInfo getTimeBoundaryInfo(String offlineTableName) {
      return null;
    }

    @Nullable
    @Override
    public TablePartitionInfo getTablePartitionInfo(String tableNameWithType) {
      return null;
    }

    @Nullable
    @Override
    public TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo(String tableNameWithType) {
      return null;
    }

    @Override
    public Set<String> getServingInstances(String tableNameWithType) {
      return new HashSet<>(_serverInstanceMap.keySet());
    }

    @Override
    public boolean isTableDisabled(String tableNameWithType) {
      return false;
    }
  }
}

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
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;


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

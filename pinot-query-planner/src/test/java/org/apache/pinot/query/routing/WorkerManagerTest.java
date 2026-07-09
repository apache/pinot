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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
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
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
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
    RoutingTable emptyRoutingTable = new RoutingTable(Map.of(), List.of(), 0);

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
    when(tableCache.getTableConfig("emptyTable_OFFLINE")).thenReturn(mock(TableConfig.class));

    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, routingManager);
    QueryEnvironment queryEnvironment = new QueryEnvironment(CommonConstants.DEFAULT_DATABASE, tableCache,
        workerManager);

    // This query requires a singleton worker (due to LIMIT) and uses useLeafServerForIntermediateStage
    // When querying an empty table, there are no leaf servers, so we need to fall back to enabled servers
    String query = "SET useLeafServerForIntermediateStage=true; SELECT * FROM emptyTable LIMIT 10";

    // This should not throw "bound must be positive" error anymore
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(query)) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertNotNull(dispatchableSubPlan);
    }
  }

  @Test
  public void testBrokerPruningUsesFilteredRoutingQueryOnThisPath() {
    Schema schema = getSchemaBuilder("testTable").build();
    ServerInstance server = getServerInstance("localhost", 1);
    Map<String, ServerInstance> serverInstanceMap = Map.of(server.getInstanceId(), server);
    RoutingTable routingTable = new RoutingTable(Map.of(server, new SegmentsToQuery(List.of("segment1"), List.of())),
        List.of(), 0);
    CapturingRoutingManager routingManager = new CapturingRoutingManager(serverInstanceMap,
        Map.of("testTable_OFFLINE", routingTable));

    Map<String, String> tableNameMap = new HashMap<>();
    tableNameMap.put("testTable_OFFLINE", "testTable_OFFLINE");
    tableNameMap.put("testTable", "testTable");

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableNameMap()).thenReturn(tableNameMap);
    when(tableCache.getActualTableName(anyString())).thenAnswer(inv -> tableNameMap.get(inv.getArgument(0)));
    when(tableCache.getSchema(anyString())).thenReturn(schema);
    when(tableCache.getTableConfig("testTable_OFFLINE")).thenReturn(mock(TableConfig.class));

    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, routingManager);
    QueryEnvironment queryEnvironment = new QueryEnvironment(CommonConstants.DEFAULT_DATABASE, tableCache,
        workerManager);

    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertNotNull(dispatchableSubPlan);
    }

    BrokerRequest brokerRequest = routingManager.getCapturedRoutingRequest("testTable_OFFLINE");
    assertNotNull(brokerRequest);
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    assertNotNull(filterExpression);
    assertEquals(filterExpression.getFunctionCall().getOperator(), "EQUALS");
    assertEquals(filterExpression.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col1");
    assertEquals(brokerRequest.getPinotQuery().getSelectList().size(), 1);
    assertEquals(brokerRequest.getPinotQuery().getSelectList().get(0).getIdentifier().getName(), "col2");
  }

  @Test
  public void testBrokerPruningRoutesFilterToBothHybridTableTypesOnThisPath() {
    Schema schema = getSchemaBuilder("testTable").build();
    ServerInstance server1 = getServerInstance("localhost", 1);
    ServerInstance server2 = getServerInstance("localhost", 2);
    Map<String, ServerInstance> serverInstanceMap = Map.of(
        server1.getInstanceId(), server1, server2.getInstanceId(), server2);
    RoutingTable offlineRoutingTable = new RoutingTable(
        Map.of(server1, new SegmentsToQuery(List.of("offline_seg1"), List.of())), List.of(), 0);
    RoutingTable realtimeRoutingTable = new RoutingTable(
        Map.of(server2, new SegmentsToQuery(List.of("realtime_seg1"), List.of())), List.of(), 0);
    CapturingRoutingManager routingManager = new CapturingRoutingManager(serverInstanceMap,
        Map.of("testTable_OFFLINE", offlineRoutingTable, "testTable_REALTIME", realtimeRoutingTable));

    Map<String, String> tableNameMap = new HashMap<>();
    tableNameMap.put("testTable_OFFLINE", "testTable_OFFLINE");
    tableNameMap.put("testTable_REALTIME", "testTable_REALTIME");
    tableNameMap.put("testTable", "testTable");

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableNameMap()).thenReturn(tableNameMap);
    when(tableCache.getActualTableName(anyString())).thenAnswer(inv -> tableNameMap.get(inv.getArgument(0)));
    when(tableCache.getSchema(anyString())).thenReturn(schema);
    when(tableCache.getTableConfig("testTable_OFFLINE")).thenReturn(mock(TableConfig.class));
    when(tableCache.getTableConfig("testTable_REALTIME")).thenReturn(mock(TableConfig.class));

    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, routingManager);
    QueryEnvironment queryEnvironment = new QueryEnvironment(CommonConstants.DEFAULT_DATABASE, tableCache,
        workerManager);

    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertNotNull(dispatchableSubPlan);
    }

    // Verify both table types received the filter from the query
    for (String tableType : List.of("testTable_OFFLINE", "testTable_REALTIME")) {
      BrokerRequest brokerRequest = routingManager.getCapturedRoutingRequest(tableType);
      assertNotNull(brokerRequest, "Missing routing request for " + tableType);
      assertEquals(brokerRequest.getPinotQuery().getDataSource().getTableName(), tableType);
      Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
      assertNotNull(filterExpression, "Missing filter for " + tableType);
      assertEquals(filterExpression.getFunctionCall().getOperator(), "EQUALS");
      assertEquals(filterExpression.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col1");
    }
  }

  @Test
  public void testBrokerPruningCountPropagatedToDispatchableSubPlan() {
    Schema schema = getSchemaBuilder("testTable").build();
    ServerInstance server = getServerInstance("localhost", 1);
    Map<String, ServerInstance> serverInstanceMap = Map.of(server.getInstanceId(), server);
    // RoutingTable with 42 pruned segments
    RoutingTable routingTable = new RoutingTable(Map.of(server, new SegmentsToQuery(List.of("segment1"), List.of())),
        List.of(), 42);
    CapturingRoutingManager routingManager = new CapturingRoutingManager(serverInstanceMap,
        Map.of("testTable_OFFLINE", routingTable));

    Map<String, String> tableNameMap = new HashMap<>();
    tableNameMap.put("testTable_OFFLINE", "testTable_OFFLINE");
    tableNameMap.put("testTable", "testTable");

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableNameMap()).thenReturn(tableNameMap);
    when(tableCache.getActualTableName(anyString())).thenAnswer(inv -> tableNameMap.get(inv.getArgument(0)));
    when(tableCache.getSchema(anyString())).thenReturn(schema);
    when(tableCache.getTableConfig("testTable_OFFLINE")).thenReturn(mock(TableConfig.class));

    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, routingManager);
    QueryEnvironment queryEnvironment = new QueryEnvironment(CommonConstants.DEFAULT_DATABASE, tableCache,
        workerManager);

    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertNotNull(dispatchableSubPlan);
      // Pruned count should propagate from RoutingTable through DispatchablePlanContext to DispatchableSubPlan
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 42);
    }
  }

  @Test
  public void testBrokerPruningOnByDefaultAndExplicitlyDisabledOnThisPath() {
    Schema schema = getSchemaBuilder("testTable").build();
    ServerInstance server = getServerInstance("localhost", 1);
    Map<String, ServerInstance> serverInstanceMap = Map.of(server.getInstanceId(), server);
    RoutingTable routingTable = new RoutingTable(Map.of(server, new SegmentsToQuery(List.of("segment1"), List.of())),
        List.of(), 0);
    CapturingRoutingManager routingManager = new CapturingRoutingManager(serverInstanceMap,
        Map.of("testTable_OFFLINE", routingTable));

    Map<String, String> tableNameMap = new HashMap<>();
    tableNameMap.put("testTable_OFFLINE", "testTable_OFFLINE");
    tableNameMap.put("testTable", "testTable");

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableNameMap()).thenReturn(tableNameMap);
    when(tableCache.getActualTableName(anyString())).thenAnswer(inv -> tableNameMap.get(inv.getArgument(0)));
    when(tableCache.getSchema(anyString())).thenReturn(schema);
    when(tableCache.getTableConfig("testTable_OFFLINE")).thenReturn(mock(TableConfig.class));

    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, routingManager);
    QueryEnvironment queryEnvironment = new QueryEnvironment(CommonConstants.DEFAULT_DATABASE, tableCache,
        workerManager);

    // Broker pruning is on by default: without any SET, the routing query should carry the leaf filter.
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SELECT col2 FROM testTable WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertNotNull(dispatchableSubPlan);
    }
    BrokerRequest brokerRequest = routingManager.getCapturedRoutingRequest("testTable_OFFLINE");
    assertNotNull(brokerRequest);
    assertNotNull(brokerRequest.getPinotQuery().getFilterExpression());

    // Explicitly disabling falls back to unfiltered SELECT * routing (segment lookup only).
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=false; SELECT col2 FROM testTable WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertNotNull(dispatchableSubPlan);
    }
    brokerRequest = routingManager.getCapturedRoutingRequest("testTable_OFFLINE");
    assertNotNull(brokerRequest);
    assertNull(brokerRequest.getPinotQuery().getFilterExpression());
  }

  @Test
  public void testBrokerPruningWrapsBooleanScalarFunctionPredicate() {
    // Regression: a boolean scalar function used directly as a predicate (WHERE contains(...)) is not a FilterKind,
    // and segment pruners resolve filter operators via FilterKind.valueOf. The routing query must carry it wrapped
    // as EQUALS(contains(...), true); before this was handled, such queries failed to plan with
    // "No enum constant FilterKind.contains" when broker pruning was enabled.
    Schema schema = getSchemaBuilder("testTable").build();
    ServerInstance server = getServerInstance("localhost", 1);
    Map<String, ServerInstance> serverInstanceMap = Map.of(server.getInstanceId(), server);
    RoutingTable routingTable = new RoutingTable(Map.of(server, new SegmentsToQuery(List.of("segment1"), List.of())),
        List.of(), 0);
    CapturingRoutingManager routingManager = new CapturingRoutingManager(serverInstanceMap,
        Map.of("testTable_OFFLINE", routingTable));

    QueryEnvironment queryEnvironment = newQueryEnvironment(schema, routingManager);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SELECT col2 FROM testTable WHERE contains(col1, 'foo')")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertNotNull(dispatchableSubPlan);
    }

    BrokerRequest brokerRequest = routingManager.getCapturedRoutingRequest("testTable_OFFLINE");
    assertNotNull(brokerRequest);
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    assertNotNull(filterExpression);
    assertEquals(filterExpression.getFunctionCall().getOperator(), "EQUALS");
    Expression wrappedFunction = filterExpression.getFunctionCall().getOperands().get(0);
    assertNotNull(wrappedFunction.getFunctionCall());
    assertEquals(wrappedFunction.getFunctionCall().getOperator(), "contains");
  }

  @Test
  public void testBrokerPruningIgnoresFilterAboveLeafAggregate() {
    // is_partitioned_by_group_by_keys produces a DIRECT (un-split) aggregate with no exchange under it, so the
    // HAVING filter lands in the SAME leaf fragment, above the aggregate. Its InputRefs index the aggregate's
    // OUTPUT row space ([col1, SUM(col3)]), not the scan columns: folding it into the routing query would
    // mis-resolve the refs against scan columns AND overwrite the genuine WHERE filter, causing incorrect
    // pruning. The routing query must carry exactly the WHERE filter and nothing above the aggregate boundary.
    Schema schema = getSchemaBuilder("testTable").build();
    ServerInstance server = getServerInstance("localhost", 1);
    Map<String, ServerInstance> serverInstanceMap = Map.of(server.getInstanceId(), server);
    RoutingTable routingTable = new RoutingTable(Map.of(server, new SegmentsToQuery(List.of("segment1"), List.of())),
        List.of(), 0);
    CapturingRoutingManager routingManager = new CapturingRoutingManager(serverInstanceMap,
        Map.of("testTable_OFFLINE", routingTable));

    QueryEnvironment queryEnvironment = newQueryEnvironment(schema, routingManager);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SELECT /*+ aggOptions(is_partitioned_by_group_by_keys='true') */ col1, SUM(col3) FROM testTable "
            + "WHERE col2 = 'x' GROUP BY col1 HAVING SUM(col3) > 10")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertNotNull(dispatchableSubPlan);
    }

    BrokerRequest brokerRequest = routingManager.getCapturedRoutingRequest("testTable_OFFLINE");
    assertNotNull(brokerRequest);
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    assertNotNull(filterExpression);
    assertEquals(filterExpression.getFunctionCall().getOperator(), "EQUALS");
    assertEquals(filterExpression.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col2");
  }

  @Test
  public void testBrokerPruningAllPrunedLeafPlansAcrossExchangeShapes() {
    // When the filter prunes every segment, the leaf gets zero workers. Planning (including mailbox assignment,
    // which runs before the all-leaves-empty short-circuit rewrite) must still succeed for every exchange shape a
    // leaf can feed: plain select, global sort/limit (singleton receiver), aggregations, empty OVER() windows and
    // set-ops. A planning exception here is a regression: the same query planned fine with pruning off.
    List<String> queries = List.of(
        "SELECT col2 FROM testTable WHERE col1 = 'foo'",
        "SELECT col2 FROM testTable WHERE col1 = 'foo' ORDER BY col2 LIMIT 5",
        "SELECT COUNT(*) FROM testTable WHERE col1 = 'foo'",
        "SELECT col1, COUNT(*) FROM testTable WHERE col1 = 'foo' GROUP BY col1 ORDER BY COUNT(*) LIMIT 3",
        "SELECT SUM(col3) OVER () FROM testTable WHERE col1 = 'foo'",
        "SELECT col2 FROM testTable WHERE col1 = 'foo' UNION ALL SELECT col2 FROM testTable",
        "SELECT DISTINCT col2 FROM testTable WHERE col1 = 'foo' LIMIT 4",
        // Dynamic-broadcast semi-join: the build side (subquery) is a separate prunable leaf feeding a
        // PIPELINE_BREAKER exchange into the main-scan leaf.
        "SELECT /*+ joinOptions(join_strategy='dynamic_broadcast') */ col2 FROM testTable "
            + "WHERE col2 IN (SELECT col2 FROM testTable WHERE col1 = 'foo')");
    for (String query : queries) {
      Schema schema = getSchemaBuilder("testTable").build();
      ServerInstance server = getServerInstance("localhost", 1);
      Map<String, ServerInstance> serverInstanceMap = Map.of(server.getInstanceId(), server);
      RoutingTable routingTable = new RoutingTable(Map.of(server, new SegmentsToQuery(List.of("segment1"),
          List.of())), List.of(), 0);
      CapturingRoutingManager routingManager = new CapturingRoutingManager(serverInstanceMap,
          Map.of("testTable_OFFLINE", routingTable));
      routingManager.setEmptyOnFilteredRouting(true);

      QueryEnvironment queryEnvironment = newQueryEnvironment(schema, routingManager);
      try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(query)) {
        DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
        assertNotNull(dispatchableSubPlan, "Planning failed for all-pruned query: " + query);
      } catch (RuntimeException e) {
        throw new AssertionError("All-pruned leaf broke planning for query: " + query + " -- " + e, e);
      }
    }
  }

  @Test
  public void testBrokerPruningFallsBackToUnfilteredRoutingOnRoutingFailure() {
    // Pruning is best-effort: if routing the filtered query throws (e.g. a segment pruner failing on an exotic
    // filter shape), the query must still plan via the unfiltered SELECT * fallback rather than fail.
    Schema schema = getSchemaBuilder("testTable").build();
    ServerInstance server = getServerInstance("localhost", 1);
    Map<String, ServerInstance> serverInstanceMap = Map.of(server.getInstanceId(), server);
    RoutingTable routingTable = new RoutingTable(Map.of(server, new SegmentsToQuery(List.of("segment1"), List.of())),
        List.of(), 0);
    CapturingRoutingManager routingManager = new CapturingRoutingManager(serverInstanceMap,
        Map.of("testTable_OFFLINE", routingTable), true);

    QueryEnvironment queryEnvironment = newQueryEnvironment(schema, routingManager);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SELECT col2 FROM testTable WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertNotNull(dispatchableSubPlan);
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 0);
    }

    // The captured request is the fallback: unfiltered SELECT * (the filtered attempt threw and was not recorded).
    BrokerRequest brokerRequest = routingManager.getCapturedRoutingRequest("testTable_OFFLINE");
    assertNotNull(brokerRequest);
    assertNull(brokerRequest.getPinotQuery().getFilterExpression());
  }

  /**
   * Mimics how segment pruners consume a routing filter: operators are resolved via {@code FilterKind.valueOf}
   * (which throws on non-FilterKind operators, e.g. bare boolean scalar functions like {@code contains}) and
   * AND/OR/NOT operands are walked recursively. Keeps the mock routing managers honest: a routing query that would
   * crash the real segment pruners also fails the unit tests.
   */
  private static void validatePrunableFilter(@Nullable Expression expression) {
    if (expression == null || expression.getFunctionCall() == null) {
      return;
    }
    Function function = expression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());
    if (filterKind == FilterKind.AND || filterKind == FilterKind.OR || filterKind == FilterKind.NOT) {
      for (Expression operand : function.getOperands()) {
        validatePrunableFilter(operand);
      }
    }
  }

  /** Builds a QueryEnvironment over a single offline table "testTable" backed by the given routing manager. */
  private static QueryEnvironment newQueryEnvironment(Schema schema, RoutingManager routingManager) {
    Map<String, String> tableNameMap = new HashMap<>();
    tableNameMap.put("testTable_OFFLINE", "testTable_OFFLINE");
    tableNameMap.put("testTable", "testTable");
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableNameMap()).thenReturn(tableNameMap);
    when(tableCache.getActualTableName(anyString())).thenAnswer(inv -> tableNameMap.get(inv.getArgument(0)));
    when(tableCache.getSchema(anyString())).thenReturn(schema);
    when(tableCache.getTableConfig("testTable_OFFLINE")).thenReturn(mock(TableConfig.class));
    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, routingManager);
    return new QueryEnvironment(CommonConstants.DEFAULT_DATABASE, tableCache, workerManager);
  }

  @Test
  public void testBrokerPruningPreservesQueryOptionsOnRoutingRequest() {
    Schema schema = getSchemaBuilder("testTable").build();
    ServerInstance server = getServerInstance("localhost", 1);
    Map<String, ServerInstance> serverInstanceMap = Map.of(server.getInstanceId(), server);
    RoutingTable routingTable = new RoutingTable(Map.of(server, new SegmentsToQuery(List.of("segment1"), List.of())),
        List.of(), 0);
    CapturingRoutingManager routingManager = new CapturingRoutingManager(serverInstanceMap,
        Map.of("testTable_OFFLINE", routingTable));

    Map<String, String> tableNameMap = new HashMap<>();
    tableNameMap.put("testTable_OFFLINE", "testTable_OFFLINE");
    tableNameMap.put("testTable", "testTable");

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableNameMap()).thenReturn(tableNameMap);
    when(tableCache.getActualTableName(anyString())).thenAnswer(inv -> tableNameMap.get(inv.getArgument(0)));
    when(tableCache.getSchema(anyString())).thenReturn(schema);
    when(tableCache.getTableConfig("testTable_OFFLINE")).thenReturn(mock(TableConfig.class));

    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, routingManager);
    QueryEnvironment queryEnvironment = new QueryEnvironment(CommonConstants.DEFAULT_DATABASE, tableCache,
        workerManager);

    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SET useLeafServerForIntermediateStage=true;"
            + " SELECT col2 FROM testTable WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertNotNull(dispatchableSubPlan);
    }

    BrokerRequest brokerRequest = routingManager.getCapturedRoutingRequest("testTable_OFFLINE");
    assertNotNull(brokerRequest);
    // Query options must be preserved on the broker-pruning routing path so that
    // routing-affecting options are visible to the routing manager.
    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    assertNotNull(queryOptions);
    assertEquals(queryOptions.get("useLeafServerForIntermediateStage"), "true");
  }

  // ---------------------------------------------------------------------------
  // Broker pruning: partitioned leaf path
  // ---------------------------------------------------------------------------

  private static final String PARTITIONED_TABLE = "testTable";
  private static final String PARTITIONED_TABLE_OFFLINE = "testTable_OFFLINE";

  @Test
  public void testBrokerPruningPartitionedLeafPrunesNonMatchingPartitions() {
    // 4 partitions, one segment + one server each. The routing manager reports only partition 2's segment survives.
    QueryEnvironment queryEnvironment =
        newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, List.of("seg2"), /*reportedPrunedByRouting=*/999);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      // seg0, seg1, seg3 pruned (3). The count is computed from dropped partitions, not the routing table's own
      // numPrunedSegments (999), proving the partitioned path computes it independently.
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 3);
      DispatchablePlanFragment leaf = leafFragment(dispatchableSubPlan);
      assertNotNull(leaf);
      assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 1);
      assertEquals(assignedSegments(leaf), List.of("seg2"));
    }
  }

  @Test
  public void testBrokerPruningPartitionedLeafKeepsMultipleMatchingPartitions() {
    QueryEnvironment queryEnvironment =
        newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, List.of("seg1", "seg3"), 0);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      // seg0, seg2 pruned.
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 2);
      DispatchablePlanFragment leaf = leafFragment(dispatchableSubPlan);
      assertNotNull(leaf);
      assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 2);
      assertEquals(new HashSet<>(assignedSegments(leaf)), Set.of("seg1", "seg3"));
    }
  }

  @Test
  public void testBrokerPruningPartitionedLeafAllPrunedFallsBackToUnpruned() {
    // The routing manager reports that no segment survives. Rather than produce an empty worker map (which would break
    // exchanges in a multi-leaf plan), the partitioned path falls back to an unpruned assignment: the server-side
    // filter still yields the correct empty result.
    QueryEnvironment queryEnvironment = newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, List.of(), 4);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 0);
      DispatchablePlanFragment leaf = leafFragment(dispatchableSubPlan);
      assertNotNull(leaf);
      assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(new HashSet<>(assignedSegments(leaf)), Set.of("seg0", "seg1", "seg2", "seg3"));
    }
  }

  @Test
  public void testBrokerPruningPartitionedLeafOnByDefault() {
    // Broker pruning is on by default: without any SET, the partitioned leaf prunes non-matching partitions.
    QueryEnvironment queryEnvironment =
        newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, List.of("seg2"), 3);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 3);
      DispatchablePlanFragment leaf = leafFragment(dispatchableSubPlan);
      assertNotNull(leaf);
      assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 1);
    }
  }

  @Test
  public void testBrokerPruningPartitionedLeafDisabledKeepsAllPartitions() {
    // Explicitly disabling broker pruning keeps all partitions assigned.
    QueryEnvironment queryEnvironment =
        newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, List.of("seg2"), 3);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=false; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 0);
      DispatchablePlanFragment leaf = leafFragment(dispatchableSubPlan);
      assertNotNull(leaf);
      assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 4);
    }
  }

  @Test
  public void testBrokerPruningPartitionedLeafMultiplePartitionsPerWorker() {
    // 4 partitions across 2 workers (partition_size=2 => 2 partitions per worker). Worker 0 owns partitions {0, 2}
    // (colocated on server 0), worker 1 owns {1, 3} (colocated on server 1). Only worker 0's partitions survive.
    QueryEnvironment queryEnvironment =
        newPartitionedQueryEnvironment(new int[]{0, 1, 0, 1}, 2, List.of("seg0", "seg2"), 0);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='2') */ "
            + "WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      // Worker 1's partitions (seg1, seg3) are fully pruned.
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 2);
      DispatchablePlanFragment leaf = leafFragment(dispatchableSubPlan);
      assertNotNull(leaf);
      assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 1);
      assertEquals(new HashSet<>(assignedSegments(leaf)), Set.of("seg0", "seg2"));
    }
  }

  // ---------------------------------------------------------------------------
  // Broker pruning: logical-table leaf path (filter forwarding into per-physical-table routing requests)
  // ---------------------------------------------------------------------------

  @Test
  public void testBuildLogicalTableRoutingRequestForwardsFilter() {
    // With a routing query present, the physical table's routing request must carry the leaf filter (so segment
    // pruners can run), the typed logical table name, and the query options.
    PinotQuery routingPinotQuery =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT col2 FROM someTable WHERE col1 = 'foo'").getPinotQuery();
    BrokerRequest brokerRequest = WorkerManager.buildLogicalTableRoutingBrokerRequest("logicalTable_OFFLINE",
        routingPinotQuery, Map.of("useLeafServerForIntermediateStage", "true"));

    assertEquals(brokerRequest.getPinotQuery().getDataSource().getTableName(), "logicalTable_OFFLINE");
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    assertNotNull(filterExpression);
    assertEquals(filterExpression.getFunctionCall().getOperator(), "EQUALS");
    assertEquals(filterExpression.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col1");
    assertEquals(brokerRequest.getPinotQuery().getQueryOptions().get("useLeafServerForIntermediateStage"), "true");
    // The source routing query must not be mutated (the method deep-copies before rewriting the table name).
    assertEquals(routingPinotQuery.getDataSource().getTableName(), "someTable");
  }

  @Test
  public void testBuildLogicalTableRoutingRequestWithoutFilterUsesSelectStar() {
    // With no routing query (pruning disabled/unsupported), a bare SELECT * request is used so no pruning occurs.
    BrokerRequest brokerRequest =
        WorkerManager.buildLogicalTableRoutingBrokerRequest("logicalTable_REALTIME", null, Map.of());
    assertEquals(brokerRequest.getPinotQuery().getDataSource().getTableName(), "logicalTable_REALTIME");
    assertNull(brokerRequest.getPinotQuery().getFilterExpression());
  }

  @Test
  public void testBrokerPruningPartitionedLeafSkippedForColocatedJoin() {
    // A pre-partitioned leaf (here both sides of a colocated self-join) feeds a 1-to-1 direct exchange wired by worker
    // id. Compacting a side's workers for pruned partitions could pair mismatched partitions across the exchange, so
    // pruning is skipped for any pre-partitioned leaf and all partitions stay assigned on every scan.
    QueryEnvironment queryEnvironment =
        newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, List.of("seg2"), 3);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT t1.col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ t1 "
            + "JOIN testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ t2 "
            + "ON t1.col1 = t2.col1 WHERE t1.col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 0);
      List<DispatchablePlanFragment> leafFragments = leafFragments(dispatchableSubPlan);
      assertFalse(leafFragments.isEmpty());
      for (DispatchablePlanFragment leaf : leafFragments) {
        assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 4, "Expected all partitions assigned for a pruned-gated "
            + "colocated join leaf");
      }
    }
  }

  @Test
  public void testBrokerPruningPartitionedLeafKeepsPartitionWithUnavailableSegment() {
    // Partition 2's segment survives; partition 1's segment is merely unavailable (not pruned). A partition must be
    // kept when its only segment is unavailable, so we never drop data that a transient outage hid -- only partitions
    // 0 and 3 (whose segments were actually pruned) are dropped.
    QueryEnvironment queryEnvironment = newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, 1, List.of("seg2"),
        List.of("seg1"), 0, false);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      // Only seg0 and seg3 pruned; seg1 is unavailable (kept), seg2 survives.
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 2);
      DispatchablePlanFragment leaf = leafFragment(dispatchableSubPlan);
      assertNotNull(leaf);
      assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 2);
      assertEquals(new HashSet<>(assignedSegments(leaf)), Set.of("seg1", "seg2"));
    }
  }

  @Test
  public void testBrokerPruningPartitionedLeafFallsBackWhenRoutingFails() {
    // If the routing call used to compute partition survival fails, pruning is best-effort and must fall back to the
    // unpruned assignment rather than failing the query.
    QueryEnvironment queryEnvironment =
        newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, 1, List.of(), List.of(), 0, /*throwOnRouting=*/true);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 0);
      DispatchablePlanFragment leaf = leafFragment(dispatchableSubPlan);
      assertNotNull(leaf);
      assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 4);
    }
  }

  @Test
  public void testBrokerPruningPartitionedLeafKeepsServerPlacementStable() {
    // Every partition is fully replicated on the same two servers, so pickEnabledServer's choice depends on the
    // per-partition seed. Pruning partition 0 must NOT shift the surviving partitions' server assignments -- the seed
    // is requestId + partitionId, not a running counter that would shift when earlier partitions are skipped. Assert
    // each surviving segment lands on the same server whether or not partition 0 was pruned.
    QueryEnvironment unprunedEnv = newPartitionedQueryEnvironment(new int[]{0, 0, 0, 0}, 2, 2,
        List.of("seg0", "seg1", "seg2", "seg3"), List.of(), 0, false);
    Map<String, String> unprunedPlacement;
    try (QueryEnvironment.CompiledQuery compiledQuery = unprunedEnv.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      unprunedPlacement = segmentToServer(leafFragment(compiledQuery.planQuery(0).getQueryPlan()));
    }
    assertEquals(unprunedPlacement.size(), 4);

    // Prune partition 0 (only seg1/seg2/seg3 survive).
    QueryEnvironment prunedEnv = newPartitionedQueryEnvironment(new int[]{0, 0, 0, 0}, 2, 2,
        List.of("seg1", "seg2", "seg3"), List.of(), 0, false);
    Map<String, String> prunedPlacement;
    try (QueryEnvironment.CompiledQuery compiledQuery = prunedEnv.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      prunedPlacement = segmentToServer(leafFragment(compiledQuery.planQuery(0).getQueryPlan()));
    }

    assertEquals(prunedPlacement.size(), 3);
    for (String segment : List.of("seg1", "seg2", "seg3")) {
      assertEquals(prunedPlacement.get(segment), unprunedPlacement.get(segment),
          "Pruning partition 0 shifted the server assignment of surviving " + segment);
    }
  }

  @Test
  public void testBrokerPruningPartitionedLeafHybridTable() {
    // Hybrid table: partition p holds offline segment segO{p} and realtime segment segR{p}, both colocated on
    // server p. The routing manager reports segO2 surviving on the offline side and segR1 on the realtime side, so
    // partitions {1, 2} are kept (a partition survives if a segment of EITHER type matches) and {0, 3} are pruned.
    QueryEnvironment queryEnvironment = newHybridPartitionedQueryEnvironment(List.of("segO2"), List.of("segR1"));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      // Pruned = both segments of partitions 0 and 3.
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 4);
      DispatchablePlanFragment leaf = leafFragment(dispatchableSubPlan);
      assertNotNull(leaf);
      assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 2);
      // Pruning is partition-level: surviving partitions dispatch ALL their segments, so the non-matching segO1 and
      // segR2 are still included alongside the matching segR1 and segO2.
      assertEquals(new HashSet<>(assignedSegments(leaf)), Set.of("segO1", "segR1", "segO2", "segR2"));
    }
  }

  /**
   * Builds a QueryEnvironment for a hybrid partitioned table "testTable" (function Hashcode on col1, 4 partitions).
   * Partition {@code p} holds offline segment {@code "segO{p}"} and realtime segment {@code "segR{p}"}, both fully
   * replicated on server {@code p}. The given surviving segment lists are what the {@link RoutingManager} returns for
   * the filtered routing query of each table type.
   */
  private static QueryEnvironment newHybridPartitionedQueryEnvironment(List<String> survivingOfflineSegments,
      List<String> survivingRealtimeSegments) {
    int numPartitions = 4;
    ServerInstance[] servers = new ServerInstance[numPartitions];
    Map<String, ServerInstance> enabledServers = new HashMap<>();
    for (int i = 0; i < numPartitions; i++) {
      servers[i] = getServerInstance("localhost", i + 1);
      enabledServers.put(servers[i].getInstanceId(), servers[i]);
    }
    TablePartitionReplicatedServersInfo.PartitionInfo[] offlinePartitions =
        new TablePartitionReplicatedServersInfo.PartitionInfo[numPartitions];
    TablePartitionReplicatedServersInfo.PartitionInfo[] realtimePartitions =
        new TablePartitionReplicatedServersInfo.PartitionInfo[numPartitions];
    for (int p = 0; p < numPartitions; p++) {
      Set<String> partitionServers = Set.of(servers[p].getInstanceId());
      offlinePartitions[p] = new TablePartitionReplicatedServersInfo.PartitionInfo(partitionServers,
          List.of("segO" + p));
      realtimePartitions[p] = new TablePartitionReplicatedServersInfo.PartitionInfo(partitionServers,
          List.of("segR" + p));
    }
    String realtimeTableName = PARTITIONED_TABLE + "_REALTIME";
    TablePartitionReplicatedServersInfo offlineInfo = new TablePartitionReplicatedServersInfo(
        PARTITIONED_TABLE_OFFLINE, "col1", "Hashcode", numPartitions, offlinePartitions, List.of());
    TablePartitionReplicatedServersInfo realtimeInfo = new TablePartitionReplicatedServersInfo(
        realtimeTableName, "col1", "Hashcode", numPartitions, realtimePartitions, List.of());

    PartitionedRoutingManager routingManager = new PartitionedRoutingManager(enabledServers,
        Map.of(PARTITIONED_TABLE_OFFLINE, offlineInfo, realtimeTableName, realtimeInfo),
        Map.of(PARTITIONED_TABLE_OFFLINE, hybridRoutingTable(servers, survivingOfflineSegments),
            realtimeTableName, hybridRoutingTable(servers, survivingRealtimeSegments)),
        false, new TimeBoundaryInfo("col3", "100"));

    Map<String, String> tableNameMap = new HashMap<>();
    tableNameMap.put(PARTITIONED_TABLE_OFFLINE, PARTITIONED_TABLE_OFFLINE);
    tableNameMap.put(realtimeTableName, realtimeTableName);
    tableNameMap.put(PARTITIONED_TABLE, PARTITIONED_TABLE);
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableNameMap()).thenReturn(tableNameMap);
    when(tableCache.getActualTableName(anyString())).thenAnswer(inv -> tableNameMap.get(inv.getArgument(0)));
    when(tableCache.getSchema(anyString())).thenReturn(getSchemaBuilder(PARTITIONED_TABLE).build());
    when(tableCache.getTableConfig(PARTITIONED_TABLE_OFFLINE)).thenReturn(mock(TableConfig.class));
    when(tableCache.getTableConfig(realtimeTableName)).thenReturn(mock(TableConfig.class));

    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 5, routingManager);
    return new QueryEnvironment(CommonConstants.DEFAULT_DATABASE, tableCache, workerManager);
  }

  /** Buckets the given surviving segments onto their owning server (partition p's segment lives on server p). */
  private static RoutingTable hybridRoutingTable(ServerInstance[] servers, List<String> survivingSegments) {
    Map<ServerInstance, List<String>> serverToSegmentList = new HashMap<>();
    for (String segment : survivingSegments) {
      int partition = Integer.parseInt(segment.substring("segO".length()));
      serverToSegmentList.computeIfAbsent(servers[partition], k -> new ArrayList<>()).add(segment);
    }
    Map<ServerInstance, SegmentsToQuery> serverToSegments = new HashMap<>();
    serverToSegmentList.forEach((server, segments) -> serverToSegments.put(server,
        new SegmentsToQuery(segments, List.of())));
    return new RoutingTable(serverToSegments, List.of(), 0);
  }

  private static QueryEnvironment newPartitionedQueryEnvironment(int[] serverIdxPerPartition, int numServers,
      List<String> survivingSegments, int reportedPrunedByRouting) {
    return newPartitionedQueryEnvironment(serverIdxPerPartition, numServers, 1, survivingSegments, List.of(),
        reportedPrunedByRouting, false);
  }

  /**
   * Builds a QueryEnvironment for an offline partitioned table "testTable" (function Hashcode on col1). Partition
   * {@code p} holds one segment {@code "seg{p}"} fully replicated on the {@code replicasPerPartition} servers starting
   * at {@code serverIdxPerPartition[p]}. {@code survivingSegments} is what the {@link RoutingManager} returns from
   * getRoutingTable for the filtered routing query -- i.e. the segments that survive broker pruning; the corresponding
   * partitions are kept. {@code unavailableSegments} are reported by the routing table as unavailable (they must keep
   * their partition alive). When {@code throwOnRouting} is true, getRoutingTable throws to exercise the fail-open path.
   */
  private static QueryEnvironment newPartitionedQueryEnvironment(int[] serverIdxPerPartition, int numServers,
      int replicasPerPartition, List<String> survivingSegments, List<String> unavailableSegments,
      int reportedPrunedByRouting, boolean throwOnRouting) {
    int numPartitions = serverIdxPerPartition.length;
    ServerInstance[] servers = new ServerInstance[numServers];
    Map<String, ServerInstance> enabledServers = new HashMap<>();
    for (int i = 0; i < numServers; i++) {
      servers[i] = getServerInstance("localhost", i + 1);
      enabledServers.put(servers[i].getInstanceId(), servers[i]);
    }
    TablePartitionReplicatedServersInfo.PartitionInfo[] partitionInfoMap =
        new TablePartitionReplicatedServersInfo.PartitionInfo[numPartitions];
    for (int p = 0; p < numPartitions; p++) {
      Set<String> fullyReplicatedServers = new HashSet<>();
      for (int r = 0; r < replicasPerPartition; r++) {
        fullyReplicatedServers.add(servers[serverIdxPerPartition[p] + r].getInstanceId());
      }
      partitionInfoMap[p] =
          new TablePartitionReplicatedServersInfo.PartitionInfo(fullyReplicatedServers, List.of("seg" + p));
    }
    TablePartitionReplicatedServersInfo tablePartitionInfo = new TablePartitionReplicatedServersInfo(
        PARTITIONED_TABLE_OFFLINE, "col1", "Hashcode", numPartitions, partitionInfoMap, List.of());

    // Model the pruned routing table: surviving segments bucketed onto their owning server.
    Map<ServerInstance, List<String>> serverToSegmentList = new HashMap<>();
    for (String segment : survivingSegments) {
      int partition = Integer.parseInt(segment.substring("seg".length()));
      serverToSegmentList.computeIfAbsent(servers[serverIdxPerPartition[partition]], k -> new ArrayList<>())
          .add(segment);
    }
    Map<ServerInstance, SegmentsToQuery> serverToSegments = new HashMap<>();
    serverToSegmentList.forEach((server, segments) -> serverToSegments.put(server,
        new SegmentsToQuery(segments, List.of())));
    RoutingTable prunedRoutingTable =
        new RoutingTable(serverToSegments, new ArrayList<>(unavailableSegments), reportedPrunedByRouting);

    PartitionedRoutingManager routingManager = new PartitionedRoutingManager(enabledServers,
        Map.of(PARTITIONED_TABLE_OFFLINE, tablePartitionInfo),
        Map.of(PARTITIONED_TABLE_OFFLINE, prunedRoutingTable), throwOnRouting);

    Map<String, String> tableNameMap = new HashMap<>();
    tableNameMap.put(PARTITIONED_TABLE_OFFLINE, PARTITIONED_TABLE_OFFLINE);
    tableNameMap.put(PARTITIONED_TABLE, PARTITIONED_TABLE);
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableNameMap()).thenReturn(tableNameMap);
    when(tableCache.getActualTableName(anyString())).thenAnswer(inv -> tableNameMap.get(inv.getArgument(0)));
    when(tableCache.getSchema(anyString())).thenReturn(getSchemaBuilder(PARTITIONED_TABLE).build());
    when(tableCache.getTableConfig(PARTITIONED_TABLE_OFFLINE)).thenReturn(mock(TableConfig.class));

    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 5, routingManager);
    return new QueryEnvironment(CommonConstants.DEFAULT_DATABASE, tableCache, workerManager);
  }

  /** Returns the leaf table-scan fragment: the only fragment with segment assignments. */
  @Nullable
  private static DispatchablePlanFragment leafFragment(DispatchableSubPlan dispatchableSubPlan) {
    List<DispatchablePlanFragment> leafFragments = leafFragments(dispatchableSubPlan);
    return leafFragments.isEmpty() ? null : leafFragments.get(0);
  }

  /** Returns all fragments with segment assignments (the table-scan leaves). */
  private static List<DispatchablePlanFragment> leafFragments(DispatchableSubPlan dispatchableSubPlan) {
    List<DispatchablePlanFragment> leafFragments = new ArrayList<>();
    for (DispatchablePlanFragment fragment : dispatchableSubPlan.getQueryStageMap().values()) {
      if (!fragment.getWorkerIdToSegmentsMap().isEmpty()) {
        leafFragments.add(fragment);
      }
    }
    return leafFragments;
  }

  /** Maps each assigned segment to the instance id of the server its worker was placed on. */
  private static Map<String, String> segmentToServer(DispatchablePlanFragment leafFragment) {
    Map<Integer, String> workerIdToServer = new HashMap<>();
    for (Map.Entry<QueryServerInstance, List<Integer>> entry
        : leafFragment.getServerInstanceToWorkerIdMap().entrySet()) {
      for (Integer workerId : entry.getValue()) {
        workerIdToServer.put(workerId, entry.getKey().getInstanceId());
      }
    }
    Map<String, String> segmentToServer = new HashMap<>();
    for (Map.Entry<Integer, Map<String, List<String>>> entry : leafFragment.getWorkerIdToSegmentsMap().entrySet()) {
      String server = workerIdToServer.get(entry.getKey());
      for (List<String> segments : entry.getValue().values()) {
        for (String segment : segments) {
          segmentToServer.put(segment, server);
        }
      }
    }
    return segmentToServer;
  }

  private static List<String> assignedSegments(DispatchablePlanFragment leafFragment) {
    List<String> segments = new ArrayList<>();
    for (Map<String, List<String>> segmentsByType : leafFragment.getWorkerIdToSegmentsMap().values()) {
      segmentsByType.values().forEach(segments::addAll);
    }
    return segments;
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
    when(tableCache.getTableConfig("testTable_OFFLINE")).thenReturn(mock(TableConfig.class));

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

  private static class CapturingRoutingManager implements RoutingManager {
    private final Map<String, ServerInstance> _serverInstanceMap;
    private final Map<String, RoutingTable> _routingTableByName;
    private final boolean _throwOnFilteredRouting;
    private boolean _emptyOnFilteredRouting;
    private final Map<String, BrokerRequest> _capturedRoutingRequests = new LinkedHashMap<>();

    CapturingRoutingManager(Map<String, ServerInstance> serverInstanceMap,
        Map<String, RoutingTable> routingTableByName) {
      this(serverInstanceMap, routingTableByName, false);
    }

    CapturingRoutingManager(Map<String, ServerInstance> serverInstanceMap,
        Map<String, RoutingTable> routingTableByName, boolean throwOnFilteredRouting) {
      _serverInstanceMap = serverInstanceMap;
      _routingTableByName = routingTableByName;
      _throwOnFilteredRouting = throwOnFilteredRouting;
    }

    /** When set, filter-bearing routing requests return an all-pruned (empty) routing table. */
    void setEmptyOnFilteredRouting(boolean emptyOnFilteredRouting) {
      _emptyOnFilteredRouting = emptyOnFilteredRouting;
    }

    @Nullable
    BrokerRequest getCapturedRoutingRequest(String tableNameWithType) {
      return _capturedRoutingRequests.get(tableNameWithType);
    }

    @Override
    public Map<String, ServerInstance> getEnabledServerInstanceMap() {
      return _serverInstanceMap;
    }

    @Nullable
    @Override
    public RoutingTable getRoutingTable(BrokerRequest brokerRequest, long requestId) {
      if (_throwOnFilteredRouting && brokerRequest.getPinotQuery().getFilterExpression() != null) {
        throw new RuntimeException("Simulated routing failure for filtered request");
      }
      validatePrunableFilter(brokerRequest.getPinotQuery().getFilterExpression());
      String tableNameWithType = brokerRequest.getQuerySource().getTableName();
      _capturedRoutingRequests.put(tableNameWithType, brokerRequest);
      if (_emptyOnFilteredRouting && brokerRequest.getPinotQuery().getFilterExpression() != null) {
        return new RoutingTable(Map.of(), List.of(), 1);
      }
      return _routingTableByName.get(tableNameWithType);
    }

    @Nullable
    @Override
    public RoutingTable getRoutingTable(BrokerRequest brokerRequest, String tableNameWithType, long requestId) {
      return getRoutingTable(brokerRequest, requestId);
    }

    @Nullable
    @Override
    public List<String> getSegments(BrokerRequest brokerRequest) {
      return List.of();
    }

    @Override
    public boolean routingExists(String tableNameWithType) {
      return _routingTableByName.containsKey(tableNameWithType);
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

  /**
   * A RoutingManager for the partitioned leaf path. It exposes a {@link TablePartitionReplicatedServersInfo} per typed
   * table (driving {@code calculatePartitionTableInfo}) and returns a pre-configured {@link RoutingTable} of surviving
   * segments from getRoutingTable (simulating what the real segment pruners would return for the query filter). This
   * lets the test drive which partitions survive without depending on the pruner internals (which are tested
   * separately).
   */
  private static class PartitionedRoutingManager implements RoutingManager {
    private final Map<String, ServerInstance> _enabledServers;
    private final Map<String, TablePartitionReplicatedServersInfo> _partitionInfoByTable;
    private final Map<String, RoutingTable> _routingTableByTable;
    private final boolean _throwOnRouting;
    @Nullable
    private final TimeBoundaryInfo _timeBoundaryInfo;

    PartitionedRoutingManager(Map<String, ServerInstance> enabledServers,
        Map<String, TablePartitionReplicatedServersInfo> partitionInfoByTable,
        Map<String, RoutingTable> routingTableByTable, boolean throwOnRouting) {
      this(enabledServers, partitionInfoByTable, routingTableByTable, throwOnRouting, null);
    }

    PartitionedRoutingManager(Map<String, ServerInstance> enabledServers,
        Map<String, TablePartitionReplicatedServersInfo> partitionInfoByTable,
        Map<String, RoutingTable> routingTableByTable, boolean throwOnRouting,
        @Nullable TimeBoundaryInfo timeBoundaryInfo) {
      _enabledServers = enabledServers;
      _partitionInfoByTable = partitionInfoByTable;
      _routingTableByTable = routingTableByTable;
      _throwOnRouting = throwOnRouting;
      _timeBoundaryInfo = timeBoundaryInfo;
    }

    @Override
    public Map<String, ServerInstance> getEnabledServerInstanceMap() {
      return _enabledServers;
    }

    @Nullable
    @Override
    public RoutingTable getRoutingTable(BrokerRequest brokerRequest, long requestId) {
      if (_throwOnRouting) {
        throw new RuntimeException("Simulated routing failure");
      }
      validatePrunableFilter(brokerRequest.getPinotQuery().getFilterExpression());
      return _routingTableByTable.get(brokerRequest.getQuerySource().getTableName());
    }

    @Nullable
    @Override
    public RoutingTable getRoutingTable(BrokerRequest brokerRequest, String tableNameWithType, long requestId) {
      return getRoutingTable(brokerRequest, requestId);
    }

    @Nullable
    @Override
    public List<String> getSegments(BrokerRequest brokerRequest) {
      return List.of();
    }

    @Override
    public boolean routingExists(String tableNameWithType) {
      return _partitionInfoByTable.containsKey(tableNameWithType);
    }

    @Nullable
    @Override
    public TimeBoundaryInfo getTimeBoundaryInfo(String offlineTableName) {
      return _timeBoundaryInfo;
    }

    @Nullable
    @Override
    public TablePartitionInfo getTablePartitionInfo(String tableNameWithType) {
      return null;
    }

    @Nullable
    @Override
    public TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo(String tableNameWithType) {
      return _partitionInfoByTable.get(tableNameWithType);
    }

    @Override
    public Set<String> getServingInstances(String tableNameWithType) {
      return new HashSet<>(_enabledServers.keySet());
    }

    @Override
    public boolean isTableDisabled(String tableNameWithType) {
      return false;
    }
  }
}

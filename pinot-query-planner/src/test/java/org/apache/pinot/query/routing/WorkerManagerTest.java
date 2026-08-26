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
import org.apache.pinot.spi.config.table.TableType;
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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests for [WorkerManager].
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

  /// Tests that when useLeafServerForIntermediateStage is enabled and querying an empty table
  /// (which results in no leaf servers), the query planner falls back to using all enabled servers
  /// instead of failing.
  ///
  /// This test simulates the scenario where a table exists with routing but has no segments,
  /// resulting in an empty RoutingTable (no server instances with segments).
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
  public void testBrokerPruningPhysicalOptimizerRoutingFilterExcludesHaving() {
    // The physical optimizer (usePhysicalOptimizer=true, where broker pruning is already on by default) builds its
    // routing query via the shared LeafStageToPinotQuery. This exercises that path end-to-end: the captured routing
    // filter must carry only the WHERE predicate and must NOT contain the HAVING predicate. (On the v2 path the
    // aggregate is split across an exchange, so HAVING stays out of the leaf; this guards that the shared builder
    // produces a correct WHERE-only routing filter on the v2 path -- the un-split-aggregate boundary case that the
    // leaf-boundary break specifically protects is covered on the logical path by
    // testBrokerPruningIgnoresFilterAboveLeafAggregate and PlanNodeRoutingQueryBuilderTest.)
    Schema schema = getSchemaBuilder("testTable").build();
    ServerInstance server = getServerInstance("localhost", 1);
    Map<String, ServerInstance> serverInstanceMap = Map.of(server.getInstanceId(), server);
    RoutingTable routingTable = new RoutingTable(Map.of(server, new SegmentsToQuery(List.of("segment1"), List.of())),
        List.of(), 0);
    CapturingRoutingManager routingManager = new CapturingRoutingManager(serverInstanceMap,
        Map.of("testTable_OFFLINE", routingTable));

    QueryEnvironment queryEnvironment = newQueryEnvironment(schema, routingManager);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET usePhysicalOptimizer=true; SELECT col1, SUM(col3) FROM testTable "
            + "WHERE col2 = 'x' GROUP BY col1 HAVING SUM(col3) > 10")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertNotNull(dispatchableSubPlan);
    }

    BrokerRequest brokerRequest = routingManager.getCapturedRoutingRequest("testTable_OFFLINE");
    assertNotNull(brokerRequest, "Physical optimizer should route through the capturing routing manager");
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    assertNotNull(filterExpression);
    // The routing filter is the WHERE predicate only; the HAVING (GREATER_THAN on SUM) must not appear.
    assertFalse(containsOperatorOnColumn(filterExpression, "GREATER_THAN"),
        "HAVING predicate leaked into the physical-optimizer routing filter: " + filterExpression);
    assertTrue(containsIdentifier(filterExpression, "col2"),
        "WHERE predicate missing from the physical-optimizer routing filter: " + filterExpression);
  }

  private static boolean containsOperatorOnColumn(Expression expression, String operator) {
    if (expression == null || expression.getFunctionCall() == null) {
      return false;
    }
    Function function = expression.getFunctionCall();
    if (function.getOperator().equals(operator)) {
      return true;
    }
    for (Expression operand : function.getOperands()) {
      if (containsOperatorOnColumn(operand, operator)) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsIdentifier(Expression expression, String identifier) {
    if (expression == null) {
      return false;
    }
    if (expression.getIdentifier() != null) {
      return expression.getIdentifier().getName().equals(identifier);
    }
    if (expression.getFunctionCall() != null) {
      for (Expression operand : expression.getFunctionCall().getOperands()) {
        if (containsIdentifier(operand, identifier)) {
          return true;
        }
      }
    }
    return false;
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

  /// Mimics how segment pruners consume a routing filter: operators are resolved via `FilterKind.valueOf`
  /// (which throws on non-FilterKind operators, e.g. bare boolean scalar functions like `contains`) and
  /// AND/OR/NOT operands are walked recursively. Keeps the mock routing managers honest: a routing query that would
  /// crash the real segment pruners also fails the unit tests.
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

  /// Builds a QueryEnvironment over a single offline table "testTable" backed by the given routing manager.
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
  public void testBrokerPruningColocatedJoinDropsClassesBothSidesPrune() {
    // A pre-partitioned leaf feeds a 1-to-1 direct exchange wired by worker id, so no leaf may decide on its own what
    // to drop. The colocation pre-pass decides for the whole group instead: a class survives when ANY member still
    // holds a segment its own filter leaves, so dropping one is safe without knowing which operator sits above.
    //
    // This is the shape the reduction is worth having for. The filter is on the partition key, so it transfers across
    // the join equality to both sides, both prune every class but 2, and the group's class list shrinks to [2]: one
    // worker per leaf, and the query is dispatched to the single server holding partition 2 instead of all four.
    QueryEnvironment queryEnvironment =
        newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, List.of("seg2"), 3);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT t1.col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ t1 "
            + "JOIN testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ t2 "
            + "ON t1.col1 = t2.col1 WHERE t1.col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      List<DispatchablePlanFragment> leafFragments = leafFragments(dispatchableSubPlan);
      assertEquals(leafFragments.size(), 2);
      for (DispatchablePlanFragment leaf : leafFragments) {
        assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 1);
        // Worker 0 stands for class 2 on both members, and carries that class's whole segment list.
        assertEquals(assignedSegments(leaf, 0), List.of("seg2"));
      }
      // Three classes dropped, one segment each, counted once per member of the group rather than from the routing
      // table's own self-reported count (the fixture reports 3).
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 6);
      // The point of the exercise: partition 2 lives only on server 3, so that is the whole dispatched set.
      assertEquals(dispatchedServers(dispatchableSubPlan), Set.of(getServerInstance("localhost", 3).getInstanceId()));
    }
  }

  @Test
  public void testBrokerPruningSelfJoinKeepsTheTwoSidesVerdictsApart() {
    // Two leaves scanning ONE table under two different filters. The pruning verdict is memoised per leaf fragment,
    // and this is what says so: keyed by table instead, one side would inherit the other's verdict and the union would
    // be computed from one filter applied twice.
    //
    // Left keeps only class 1, right only class 2, so the union is {1, 2} -- a result neither side's verdict produces
    // on its own, and neither does either verdict applied to both sides ({1} or {2}).
    QueryEnvironment queryEnvironment =
        newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, 1, List.of(), List.of(), 0, false, Set.of(), Set.of(),
            Map.of("left", List.of("seg1"), "right", List.of("seg2")));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT t1.col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ t1 "
            + "JOIN testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ t2 "
            + "ON t1.col1 = t2.col1 WHERE t1.col2 = 'left' AND t2.col2 = 'right'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      List<DispatchablePlanFragment> leafFragments = leafFragments(dispatchableSubPlan);
      assertEquals(leafFragments.size(), 2);
      Map<Integer, Integer> workerIdToClass = new HashMap<>();
      for (DispatchablePlanFragment leaf : leafFragments) {
        // Both classes survive on both sides, and a class the group keeps dispatches all of its segments even on the
        // member whose own filter excluded them.
        assertEquals(workerIdToPartitions(leaf, "seg"), Map.of(0, Set.of(1), 1, Set.of(2)));
        mergeWorkerIdToClass(workerIdToClass, leaf, "seg", 4);
      }
      assertEquals(workerIdToClass, Map.of(0, 1, 1, 2));
      // Classes 0 and 3 dropped, one segment each, on each of the two leaves.
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 4);
    }
  }

  @Test
  public void testBrokerPruningColocatedJoinKeepsClassOnlyOneSidePrunes() {
    // The union rule, and the reason it is not an intersection: only t1 is filtered, t2 still holds every class, so
    // every class keeps its worker and the fan-out is unchanged. Dropping the classes t1's filter empties would be
    // wrong the moment the operator above is a RIGHT or FULL join, a union, or an anti-join -- and the worker
    // assignment deliberately knows nothing about which one it is. A one-sided filter buying nothing is the price.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(4, false).survivingSegments(List.of("a_seg1")),
        new ColocatedTableSpec(4, false).survivingSegments(List.of("b_seg0", "b_seg1", "b_seg2", "b_seg3")));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; " + colocatedJoinQuery(4) + " WHERE " + COLOCATED_TABLE_A + ".col2 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      DispatchablePlanFragment leafA = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A);
      DispatchablePlanFragment leafB = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B);
      assertEquals(leafA.getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(leafB.getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 0);
      // A kept class dispatches all of its segments on every member, including the ones the member's own filter
      // excluded, so worker 0 of the filtered side still scans a_seg0.
      assertEquals(workerIdToPartitions(leafA, "a_seg"),
          Map.of(0, Set.of(0), 1, Set.of(1), 2, Set.of(2), 3, Set.of(3)));
      assertEquals(workerIdToServer(leafA), workerIdToServer(leafB));
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

  @Test
  public void testHybridPartitionedLeafRejectsSegmentsWithInvalidPartition() {
    // The hybrid branch merges the two sides' maps itself instead of going through
    // PartitionTableInfo.fromTablePartitionInfo, so it must run the invalid-partition check on both. Here only the
    // realtime side has such a segment.
    QueryEnvironment queryEnvironment = newHybridPartitionedQueryEnvironment(List.of("segO2"), List.of("segR1"),
        List.of(), List.of("segRbad"));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      // QueryEnvironment wraps the planning failure, so assert on the cause.
      RuntimeException e = expectThrows(RuntimeException.class, () -> compiledQuery.planQuery(0));
      Throwable cause = e.getCause();
      assertTrue(cause instanceof IllegalStateException, String.valueOf(cause));
      assertTrue(cause.getMessage().contains("segments with invalid partition"), cause.getMessage());
      assertTrue(cause.getMessage().contains("testTable_REALTIME"), cause.getMessage());
    }
  }

  @Test
  public void testPartitionedLeafRejectsPartitionWithOnlyDeferredSegments() {
    // Partition 2 has no entry in the partition info map, but not because it is empty: all of its segments are
    // deferred, so no single server can scan it whole and padding it would silently drop its rows.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(Set.of(2), Set.of(2), Set.of(2), Set.of());
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_JOIN_QUERY)) {
      RuntimeException e = expectThrows(RuntimeException.class, () -> compiledQuery.planQuery(0));
      Throwable cause = e.getCause();
      assertTrue(cause instanceof IllegalStateException, String.valueOf(cause));
      assertTrue(cause.getMessage().contains("Failed to find a fully replicated server for partitions: [2]"),
          cause.getMessage());
      // The message names the scanned (raw) table name.
      assertTrue(cause.getMessage().contains("of table: " + COLOCATED_TABLE_A), cause.getMessage());
    }
  }

  @Test
  public void testPlainPartitionedLeafRejectsPartitionWithOnlyDeferredSegmentsWithoutPruning() {
    // A plain partitioned leaf, outside any colocated group, so only the check at the assignment site can fire. Pruning
    // is off, so partition 3 needs a worker and has nowhere to go: padding it would drop the held-back segments' rows.
    QueryEnvironment queryEnvironment =
        newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, 1, List.of("seg2"), List.of(), 0, false, Set.of(3),
            Set.of(3));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=false; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      RuntimeException e = expectThrows(RuntimeException.class, () -> compiledQuery.planQuery(0));
      Throwable cause = e.getCause();
      assertTrue(cause instanceof IllegalStateException, String.valueOf(cause));
      assertTrue(cause.getMessage().contains("Failed to find a fully replicated server for partitions: [3]"),
          cause.getMessage());
      assertTrue(cause.getMessage().contains("of table: " + PARTITIONED_TABLE), cause.getMessage());
    }
  }

  @Test
  public void testPlainPartitionedLeafWithPartitionWithOnlyDeferredSegmentsStillPrunes() {
    // Same layout, with broker pruning active and a filter that only matches partition 2. The deferred partition gets
    // no worker either way, so the query must keep planning: failing every query on the table while a segment is new
    // would be a bigger regression than the rows this one cannot see.
    QueryEnvironment queryEnvironment =
        newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, 1, List.of("seg2"), List.of(), 0, false, Set.of(3),
            Set.of(3));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      DispatchablePlanFragment leaf = leafFragment(compiledQuery.planQuery(0).getQueryPlan());
      assertNotNull(leaf);
      assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 1);
      assertEquals(assignedSegments(leaf), List.of("seg2"));
    }
  }

  @Test
  public void testPlainPartitionedLeafWithEmptyPartitionPlansWhenNothingIsPruned() {
    // Partition 3 holds no segment at all and the filter prunes nothing, so the pruning verdict is an EMPTY set rather
    // than an absent one. It still has to be acted on: without a verdict every partition gets a worker, and the one
    // with no segment has no server to place it on, which fails a query that plans perfectly well with 3 workers.
    // Hence the verdict distinguishes "a filter ran and proved nothing" from "there was no filter".
    QueryEnvironment queryEnvironment =
        newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, 1, List.of("seg0", "seg1", "seg2"), List.of(), 0,
            false, Set.of(3), Set.of());
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      DispatchablePlanFragment leaf = leafFragment(dispatchableSubPlan);
      assertNotNull(leaf);
      assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 3);
      assertEquals(assignedSegments(leaf), List.of("seg0", "seg1", "seg2"));
      // Nothing was pruned, only skipped for holding nothing, so nothing is reported as pruned either.
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 0);
    }
  }

  @Test
  public void testPartitionedLeafRejectsTableWithoutAnyPartition() {
    // An empty partition info map passes the "partitions must be a multiple of the hinted partition size" check
    // trivially, leaving 0 partitions per worker, so it has to be rejected on its own.
    QueryEnvironment queryEnvironment = newPartitionedQueryEnvironment(new int[0], 4, List.of(), 0);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      RuntimeException e = expectThrows(RuntimeException.class, () -> compiledQuery.planQuery(0));
      Throwable cause = e.getCause();
      assertTrue(cause instanceof IllegalStateException, String.valueOf(cause));
      assertTrue(cause.getMessage().contains("Failed to find any partition for table: " + PARTITIONED_TABLE),
          cause.getMessage());
    }
  }

  @Test
  public void testPartitionedLeafPublishesACopyOfTheBrokerSegmentList() {
    // The lists of a one-partition-per-worker assignment come from the broker's published metadata and are handed to
    // filterLeafStageSegments, which may edit them in place, so they must be copied first.
    QueryEnvironment queryEnvironment = newPartitionedQueryEnvironment(new int[]{0, 1, 2, 3}, 4, List.of(), 0);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=false; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      DispatchablePlanFragment leaf = leafFragment(compiledQuery.planQuery(0).getQueryPlan());
      assertNotNull(leaf);
      List<String> segments = leaf.getWorkerIdToSegmentsMap().get(0).get(TableType.OFFLINE.name());
      assertEquals(segments, List.of("seg0"));
      segments.add("mutated");
      // Planning the same query again must see the broker's original list, not the mutation above.
      DispatchablePlanFragment leafAgain = leafFragment(compiledQuery.planQuery(1).getQueryPlan());
      assertNotNull(leafAgain);
      List<String> segmentsAgain = leafAgain.getWorkerIdToSegmentsMap().get(0).get(TableType.OFFLINE.name());
      assertEquals(segmentsAgain, List.of("seg0"));
      assertNotSame(segmentsAgain, segments);
    }
  }

  @Test
  public void testHybridPartitionedLeafRejectsOfflineSegmentsWithInvalidPartition() {
    // The mirror of testHybridPartitionedLeafRejectsSegmentsWithInvalidPartition: the merged map is built from both
    // sides, so the check has to run on both. Here only the offline side has such a segment.
    QueryEnvironment queryEnvironment = newHybridPartitionedQueryEnvironment(List.of("segO2"), List.of("segR1"),
        List.of("segObad"), List.of());
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      RuntimeException e = expectThrows(RuntimeException.class, () -> compiledQuery.planQuery(0));
      Throwable cause = e.getCause();
      assertTrue(cause instanceof IllegalStateException, String.valueOf(cause));
      assertTrue(cause.getMessage().contains("segments with invalid partition"), cause.getMessage());
      assertTrue(cause.getMessage().contains(PARTITIONED_TABLE_OFFLINE), cause.getMessage());
    }
  }

  @Test
  public void testHybridPartitionedLeafKeepsPartitionDeferredOnOneSideOnly() {
    // Partition 3's offline segments were all held back, but the realtime side still serves the whole partition, so the
    // merged map has an entry for it. Reporting it would fail a query the realtime side can answer on its own.
    QueryEnvironment queryEnvironment = newHybridPartitionedQueryEnvironment(List.of(), List.of(), List.of(), List.of(),
        Set.of(3), Set.of(3));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=false; SELECT col2 FROM testTable "
            + "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='4') */ "
            + "WHERE col1 = 'foo'")) {
      DispatchablePlanFragment leaf = leafFragment(compiledQuery.planQuery(0).getQueryPlan());
      assertNotNull(leaf);
      assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 4);
      // Worker 3 is realtime-only, the other 3 workers carry both table types.
      assertEquals(leaf.getWorkerIdToSegmentsMap().get(3).keySet(), Set.of(TableType.REALTIME.name()));
      assertEquals(assignedSegments(leaf, 3), List.of("segR3"));
      assertEquals(leaf.getWorkerIdToSegmentsMap().get(0).keySet(),
          Set.of(TableType.OFFLINE.name(), TableType.REALTIME.name()));
    }
  }

  @Test
  public void testColocatedJoinDropsEmptyPartitionOnBothSides() {
    // Partition 3 holds no segment on either side of the colocated join. Both leaves must drop it, keeping the same
    // worker id -> partition mapping so that the 1-to-1 exchange between them stays correct.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(Set.of(3), Set.of(3), Set.of(), Set.of());
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_JOIN_QUERY)) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      List<DispatchablePlanFragment> leafFragments = leafFragments(dispatchableSubPlan);
      assertEquals(leafFragments.size(), 2);
      for (DispatchablePlanFragment leaf : leafFragments) {
        // 3 workers instead of 4, one per surviving partition, and partition 3's (absent) segment is not assigned.
        assertEquals(leaf.getWorkerIdToSegmentsMap().size(), 3);
        String segmentPrefix = leaf.getTableName().startsWith(COLOCATED_TABLE_A) ? "a_seg" : "b_seg";
        assertEquals(new HashSet<>(assignedSegments(leaf)),
            Set.of(segmentPrefix + "0", segmentPrefix + "1", segmentPrefix + "2"));
      }
      // Worker k of both leaves must hold partition k's segment and live on partition k's server, otherwise the 1-to-1
      // exchange would pair rows of different partitions.
      Map<Integer, String> workerIdToServerA = workerIdToServer(leafFragments.get(0));
      Map<Integer, String> workerIdToServerB = workerIdToServer(leafFragments.get(1));
      assertEquals(workerIdToServerA, workerIdToServerB);
      for (int workerId = 0; workerId < 3; workerId++) {
        assertEquals(assignedSegments(leafFragments.get(0), workerId).size(), 1);
        // Partition p lives on server p, i.e. localhost:p+1 (see newColocatedJoinQueryEnvironment).
        assertTrue(workerIdToServerA.get(workerId).endsWith("_" + (workerId + 1)), workerIdToServerA.get(workerId));
      }
      // The join stage takes its workers from the leaves, so it must be reduced along with them, and it must land on
      // their servers: the exchange is still a 1-to-1 local exchange rather than a shuffle across all the servers.
      DispatchablePlanFragment joinFragment = joinFragment(dispatchableSubPlan);
      assertEquals(joinFragment.getWorkerMetadataList().size(), 3);
      assertEquals(workerIdToServer(joinFragment), workerIdToServerA);
    }
  }

  @Test
  public void testColocatedJoinPadsPartitionEmptyOnOneSide() {
    // Table A is empty in partition 3 but table B is not, so the group keeps the class and table A gets a worker with
    // no segments for it. Here the server holding B's partition 3 does not host table A at all (each partition lives on
    // its own server), so that worker cannot be placed with its peer and falls back to a server that does host A.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(Set.of(3), Set.of(), Set.of(), Set.of());
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_JOIN_QUERY)) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      List<DispatchablePlanFragment> leafFragments = leafFragments(dispatchableSubPlan);
      assertEquals(leafFragments.size(), 2);
      DispatchablePlanFragment leafA = leafFragments.get(0).getTableName().startsWith(COLOCATED_TABLE_A)
          ? leafFragments.get(0) : leafFragments.get(1);
      DispatchablePlanFragment leafB = leafA == leafFragments.get(0) ? leafFragments.get(1) : leafFragments.get(0);
      // Both sides keep all 4 workers, one per partition class, so worker k still stands for partition k on both.
      assertEquals(leafA.getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(leafB.getWorkerIdToSegmentsMap().size(), 4);
      for (int workerId = 0; workerId < 3; workerId++) {
        assertEquals(assignedSegments(leafA, workerId), List.of("a_seg" + workerId));
      }
      assertEquals(assignedSegments(leafB, 3), List.of("b_seg3"));
      // A single table type key mapped to an empty (mutable) list, on a server that hosts table A rather than on
      // partition 3's server (localhost_4), which only hosts table B's partition 3.
      Map<String, List<String>> emptyWorkerSegmentsMap = leafA.getWorkerIdToSegmentsMap().get(3);
      assertEquals(emptyWorkerSegmentsMap.keySet(), Set.of(TableType.OFFLINE.name()));
      assertEquals(emptyWorkerSegmentsMap.get(TableType.OFFLINE.name()), List.of());
      emptyWorkerSegmentsMap.get(TableType.OFFLINE.name()).add("mutable");
      String emptyWorkerServer = workerIdToServer(leafA).get(3);
      assertTrue(Set.of("_1", "_2", "_3").stream().anyMatch(emptyWorkerServer::endsWith), emptyWorkerServer);
      // The join stage takes its workers from a leaf, so it keeps all 4 workers as well.
      assertEquals(joinFragment(dispatchableSubPlan).getWorkerMetadataList().size(), 4);
    }
  }

  @Test
  public void testColocatedJoinPadsWorkerOnPeerServer() {
    // Same as above, but every server hosts every partition of both tables, so the empty worker can borrow both the
    // candidate servers and the seed of the peer holding partition 3, which is what keeps the exchange in process.
    QueryEnvironment queryEnvironment =
        newColocatedJoinQueryEnvironment(Set.of(3), Set.of(), Set.of(), Set.of(), true);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_JOIN_QUERY)) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      List<DispatchablePlanFragment> leafFragments = leafFragments(dispatchableSubPlan);
      assertEquals(leafFragments.size(), 2);
      DispatchablePlanFragment leafA = leafFragments.get(0).getTableName().startsWith(COLOCATED_TABLE_A)
          ? leafFragments.get(0) : leafFragments.get(1);
      DispatchablePlanFragment leafB = leafA == leafFragments.get(0) ? leafFragments.get(1) : leafFragments.get(0);
      assertEquals(leafA.getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(leafB.getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(assignedSegments(leafA, 3), List.of());
      assertEquals(assignedSegments(leafB, 3), List.of("b_seg3"));
      // Every worker of the padding side lands on the same server as its peer, the empty one included.
      assertEquals(workerIdToServer(leafA), workerIdToServer(leafB));
      DispatchablePlanFragment joinFragment = joinFragment(dispatchableSubPlan);
      assertEquals(workerIdToServer(joinFragment), workerIdToServer(leafA));
      // The empty worker is wired like any other one: it has an outbound mailbox and the join worker reading its class
      // has an inbound one. Dropping either would write its end-of-stream block, and any error, where nobody reads.
      int leafAFragmentId = leafA.getPlanFragment().getFragmentId();
      int joinFragmentId = joinFragment.getPlanFragment().getFragmentId();
      assertNotNull(leafA.getWorkerMetadataList().get(3).getMailboxInfosMap().get(joinFragmentId));
      assertNotNull(joinFragment.getWorkerMetadataList().get(3).getMailboxInfosMap().get(leafAFragmentId));
      // Landing with the peer is what keeps the exchange in process: the sender and the join worker reading it share
      // one local mailbox rather than a cross-server pair.
      MailboxInfos mailboxInfos =
          joinFragment.getWorkerMetadataList().get(2).getMailboxInfosMap().get(leafAFragmentId);
      assertNotNull(mailboxInfos);
      assertTrue(mailboxInfos instanceof SharedMailboxInfos, String.valueOf(mailboxInfos));
      assertEquals(mailboxInfos.getMailboxInfos().size(), 1);
      assertEquals(mailboxInfos.getMailboxInfos().get(0).getHostname(), "localhost");
      assertEquals(mailboxInfos.getMailboxInfos().get(0).getWorkerIds(), List.of(2));
    }
  }

  @Test
  public void testColocatedJoinPadsWorkerOnPeerServerRatherThanItsOwn() {
    // Borrowing the peer's candidate servers is the only thing that keeps the empty worker with its peer, and here the
    // peer's set is a strict subset of the servers hosting table A, so the two resolve differently:
    //   - table A holds partitions 0..2 on servers 1, 2 and {3, 4}, so its own candidate set is all 4 servers;
    //   - table B's partition 3 lives on server 1 alone, so borrowing lands the empty worker there;
    //   - picking from table A's own set would land it on server 3 instead.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(4, false).emptyPartitions(Set.of(3))
            .partitionServerIndexes(Map.of(0, Set.of(0), 1, Set.of(1), 2, Set.of(2, 3))),
        new ColocatedTableSpec(4, false)
            .partitionServerIndexes(Map.of(0, Set.of(0), 1, Set.of(1), 2, Set.of(2), 3, Set.of(0))));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_JOIN_QUERY)) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      DispatchablePlanFragment leafA = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A);
      DispatchablePlanFragment leafB = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B);
      // Worker 3 is the empty one: it stands for class 3, which table A holds no data in.
      assertEquals(assignedSegments(leafA, 3), List.of());
      String peerServer = getServerInstance("localhost", 1).getInstanceId();
      assertEquals(workerIdToServer(leafB).get(3), peerServer);
      // The discriminating assertions: on the peer's server, not on the one table A's own candidate set resolves to.
      assertEquals(workerIdToServer(leafA).get(3), peerServer);
      assertNotEquals(workerIdToServer(leafA).get(3), getServerInstance("localhost", 3).getInstanceId());
    }
  }

  @Test
  public void testColocatedJoinPadsRealtimeWorkerWithRealtimeSegmentsMap() {
    // The one table type key an empty worker emits must be one the chosen server actually has a table data manager for.
    // Every other colocated test registers offline tables only, so this covers the realtime branch.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(4, true).emptyPartitions(Set.of(3)), new ColocatedTableSpec(4, true),
        TableType.REALTIME);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_JOIN_QUERY)) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      DispatchablePlanFragment leafA = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A);
      Map<String, List<String>> emptyWorkerSegmentsMap = leafA.getWorkerIdToSegmentsMap().get(3);
      assertEquals(emptyWorkerSegmentsMap.keySet(), Set.of(TableType.REALTIME.name()));
      assertEquals(emptyWorkerSegmentsMap.get(TableType.REALTIME.name()), List.of());
    }
  }

  @Test
  public void testColocatedNonEquiJoinIsNotReduced() {
    // A non-equi colocated join sends one side BROADCAST with prePartitioned set, which reducing the worker count must
    // not wire 1-to-1 (see ColocationGroupAnalyzer#findReducibleGroups), so the group keeps today's assignment -- and
    // today's assignment rejects the empty partition.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(Set.of(3), Set.of(3), Set.of(), Set.of());
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_NON_EQUI_JOIN_QUERY)) {
      RuntimeException e = expectThrows(RuntimeException.class, () -> compiledQuery.planQuery(0));
      Throwable cause = e.getCause();
      assertTrue(cause instanceof IllegalStateException, String.valueOf(cause));
      assertTrue(cause.getMessage().contains("Failed to find any segment for table"), cause.getMessage());
    }
  }

  @Test
  public void testColocatedJoinPadsClassEmptyOnOneSideWithMultiplePartitionsPerWorker() {
    // 8 partitions per table over a hinted partition size of 4, so worker k handles the class {k, k + 4}. Table A holds
    // no segment in either partition of class 3, so it pads that class while table B keeps its 2 segments there.
    QueryEnvironment queryEnvironment =
        newColocatedJoinQueryEnvironment(Set.of(3, 7), Set.of(), Set.of(), Set.of(), true, 8);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_JOIN_QUERY)) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      List<DispatchablePlanFragment> leafFragments = leafFragments(dispatchableSubPlan);
      assertEquals(leafFragments.size(), 2);
      DispatchablePlanFragment leafA = leafFragments.get(0).getTableName().startsWith(COLOCATED_TABLE_A)
          ? leafFragments.get(0) : leafFragments.get(1);
      DispatchablePlanFragment leafB = leafA == leafFragments.get(0) ? leafFragments.get(1) : leafFragments.get(0);
      assertEquals(leafA.getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(leafB.getWorkerIdToSegmentsMap().size(), 4);
      // The workers of the surviving classes are not shifted by the empty one, which keeps its own class index.
      for (int workerId = 0; workerId < 3; workerId++) {
        assertEquals(new HashSet<>(assignedSegments(leafA, workerId)),
            Set.of("a_seg" + workerId, "a_seg" + (workerId + 4)));
      }
      assertEquals(assignedSegments(leafA, 3), List.of());
      assertEquals(new HashSet<>(assignedSegments(leafB, 3)), Set.of("b_seg3", "b_seg7"));
      // Same shape as on the one-partition-per-worker path: one table type key, mapped to a mutable empty list.
      Map<String, List<String>> emptyWorkerSegmentsMap = leafA.getWorkerIdToSegmentsMap().get(3);
      assertEquals(emptyWorkerSegmentsMap.keySet(), Set.of(TableType.OFFLINE.name()));
      assertEquals(emptyWorkerSegmentsMap.get(TableType.OFFLINE.name()), List.of());
      emptyWorkerSegmentsMap.get(TableType.OFFLINE.name()).add("mutable");
      // Every server hosts every partition here, so the empty worker lands with its peer.
      assertEquals(workerIdToServer(leafA), workerIdToServer(leafB));
    }
  }

  @Test
  public void testColocatedJoinRejectsFullyEmptyTable() {
    QueryEnvironment queryEnvironment =
        newColocatedJoinQueryEnvironment(Set.of(0, 1, 2, 3), Set.of(), Set.of(), Set.of());
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_JOIN_QUERY)) {
      RuntimeException e = expectThrows(RuntimeException.class, () -> compiledQuery.planQuery(0));
      Throwable cause = e.getCause();
      assertTrue(cause instanceof IllegalStateException, String.valueOf(cause));
      assertTrue(cause.getMessage().contains("Failed to find any segment in any partition for table: "
          + COLOCATED_TABLE_A), cause.getMessage());
    }
  }

  @Test
  public void testColocatedJoinAlignsWorkersWhenEmptyClassesDiffer() {
    // The case a naive "skip the empty partition" fix gets wrong: table A holds no segment in partition 1 and table B
    // holds none in partition 2, so skipping what each side is missing would leave both with 3 workers and mispair them
    // (see DispatchablePlanMetadata#getPartitionClassIds). Taking the union keeps both partitions on both sides.
    QueryEnvironment queryEnvironment =
        newColocatedJoinQueryEnvironment(Set.of(1), Set.of(2), Set.of(), Set.of(), true);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_JOIN_QUERY)) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      DispatchablePlanFragment leafA = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A);
      DispatchablePlanFragment leafB = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B);
      // Neither side dropped a class the other kept, so the worker counts agree for the right reason.
      assertEquals(leafA.getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(leafB.getWorkerIdToSegmentsMap().size(), 4);
      // Each side scans its own partition on the 3 workers it has data for, at that partition's structural index.
      assertEquals(workerIdToPartitions(leafA, "a_seg"), Map.of(0, Set.of(0), 2, Set.of(2), 3, Set.of(3)));
      assertEquals(workerIdToPartitions(leafB, "b_seg"), Map.of(0, Set.of(0), 1, Set.of(1), 3, Set.of(3)));
      // Each side pads the class only the other carries: A pads worker 1, B pads worker 2.
      assertEquals(assignedSegments(leafA, 1), List.of());
      assertEquals(assignedSegments(leafB, 2), List.of());
      // The mapping itself, not just the counts: the two sides must agree on every worker id, and cover them all.
      Map<Integer, Integer> workerIdToClass = new HashMap<>();
      mergeWorkerIdToClass(workerIdToClass, leafA, "a_seg", 4);
      mergeWorkerIdToClass(workerIdToClass, leafB, "b_seg", 4);
      assertEquals(workerIdToClass, Map.of(0, 0, 1, 1, 2, 2, 3, 3));
      // Both empty workers land with their peer, so the join still runs on the leaves' servers.
      assertEquals(workerIdToServer(leafA), workerIdToServer(leafB));
      assertEquals(workerIdToServer(joinFragment(dispatchableSubPlan)), workerIdToServer(leafA));
    }
  }

  @Test
  public void testColocatedJoinOnNonPartitionKeyAlignsWorkersWhenEmptyClassesDiffer() {
    // Same as above, joining on a column that is not the hinted partition key (see
    // #colocatedJoinQueryOnNonPartitionKey). Worker assignment reads the hinted partition key and the broker's
    // partition info, never the join condition, so this must resolve identically: which class a row sits in is decided
    // by the column the data was partitioned on, whatever column the join then matches on.
    QueryEnvironment queryEnvironment =
        newColocatedJoinQueryEnvironment(Set.of(1), Set.of(2), Set.of(), Set.of(), true);
    try (QueryEnvironment.CompiledQuery compiledQuery =
        queryEnvironment.compile(colocatedJoinQueryOnNonPartitionKey(4))) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      DispatchablePlanFragment leafA = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A);
      DispatchablePlanFragment leafB = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B);
      assertEquals(workerIdToPartitions(leafA, "a_seg"), Map.of(0, Set.of(0), 2, Set.of(2), 3, Set.of(3)));
      assertEquals(workerIdToPartitions(leafB, "b_seg"), Map.of(0, Set.of(0), 1, Set.of(1), 3, Set.of(3)));
      assertEquals(assignedSegments(leafA, 1), List.of());
      assertEquals(assignedSegments(leafB, 2), List.of());
      Map<Integer, Integer> workerIdToClass = new HashMap<>();
      mergeWorkerIdToClass(workerIdToClass, leafA, "a_seg", 4);
      mergeWorkerIdToClass(workerIdToClass, leafB, "b_seg", 4);
      assertEquals(workerIdToClass, Map.of(0, 0, 1, 1, 2, 2, 3, 3));
      assertEquals(workerIdToServer(leafA), workerIdToServer(leafB));
      // Both exchanges into the join stay 1-to-1 and in process rather than degrading to a shuffle.
      DispatchablePlanFragment joinFragment = joinFragment(dispatchableSubPlan);
      assertEquals(workerIdToServer(joinFragment), workerIdToServer(leafA));
      int leafAFragmentId = leafA.getPlanFragment().getFragmentId();
      int leafBFragmentId = leafB.getPlanFragment().getFragmentId();
      for (int workerId = 0; workerId < 4; workerId++) {
        Map<Integer, MailboxInfos> mailboxInfosMap =
            joinFragment.getWorkerMetadataList().get(workerId).getMailboxInfosMap();
        for (int senderFragmentId : List.of(leafAFragmentId, leafBFragmentId)) {
          MailboxInfos mailboxInfos = mailboxInfosMap.get(senderFragmentId);
          assertNotNull(mailboxInfos, "No mailbox for sender: " + senderFragmentId + " on worker: " + workerId);
          assertTrue(mailboxInfos instanceof SharedMailboxInfos, String.valueOf(mailboxInfos));
          assertEquals(mailboxInfos.getMailboxInfos().size(), 1);
          assertEquals(mailboxInfos.getMailboxInfos().get(0).getWorkerIds(), List.of(workerId));
        }
      }
    }
  }

  @Test
  public void testColocatedJoinOnNonPartitionKeyReducesFanOut() {
    // The fan-out reduction below, on a join key that is not the hinted partition key.
    Set<Integer> emptyPartitions = Set.of(1, 2, 3, 4, 6, 7);
    QueryEnvironment queryEnvironment =
        newColocatedJoinQueryEnvironment(emptyPartitions, emptyPartitions, Set.of(), Set.of(), false, 8);
    try (QueryEnvironment.CompiledQuery compiledQuery =
        queryEnvironment.compile(colocatedJoinQueryOnNonPartitionKey(8))) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      DispatchablePlanFragment leafA = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A);
      DispatchablePlanFragment leafB = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B);
      assertEquals(workerIdToPartitions(leafA, "a_seg"), Map.of(0, Set.of(0), 1, Set.of(5)));
      assertEquals(workerIdToPartitions(leafB, "b_seg"), Map.of(0, Set.of(0), 1, Set.of(5)));
      String server1 = getServerInstance("localhost", 1).getInstanceId();
      String server2 = getServerInstance("localhost", 2).getInstanceId();
      assertEquals(dispatchedServers(dispatchableSubPlan), Set.of(server1, server2));
    }
  }

  @Test
  public void testColocatedJoinReducesFanOutToPopulatedClasses() {
    // 8 declared partition classes but only 2 populated (partitions 0 and 5), on both sides. Each leaf gets 2 workers
    // instead of 8, and the query is only dispatched to the 2 servers holding those classes: the fan-out follows from
    // the reduced class list, because the dispatched server set is built from the worker -> server map.
    Set<Integer> emptyPartitions = Set.of(1, 2, 3, 4, 6, 7);
    QueryEnvironment queryEnvironment =
        newColocatedJoinQueryEnvironment(emptyPartitions, emptyPartitions, Set.of(), Set.of(), false, 8);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(colocatedJoinQuery(8))) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      DispatchablePlanFragment leafA = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A);
      DispatchablePlanFragment leafB = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B);
      assertEquals(leafA.getWorkerIdToSegmentsMap().size(), 2);
      assertEquals(leafB.getWorkerIdToSegmentsMap().size(), 2);
      // Worker 0 -> class 0, worker 1 -> class 5: the worker id is the index in the surviving class list.
      assertEquals(workerIdToPartitions(leafA, "a_seg"), Map.of(0, Set.of(0), 1, Set.of(5)));
      assertEquals(workerIdToPartitions(leafB, "b_seg"), Map.of(0, Set.of(0), 1, Set.of(5)));
      // Partition p is hosted by server p % 4, so only servers 1 and 2 hold the surviving classes. Nothing in the plan
      // may be dispatched to the other 2 servers.
      String server1 = getServerInstance("localhost", 1).getInstanceId();
      String server2 = getServerInstance("localhost", 2).getInstanceId();
      assertEquals(new HashSet<>(workerIdToServer(leafA).values()), Set.of(server1, server2));
      assertEquals(new HashSet<>(workerIdToServer(leafB).values()), Set.of(server1, server2));
      assertEquals(joinFragment(dispatchableSubPlan).getWorkerMetadataList().size(), 2);
      assertEquals(dispatchedServers(dispatchableSubPlan), Set.of(server1, server2));
    }
  }

  @Test
  public void testBrokerPruningColocatedJoinDropsClassWhosePartitionsListNoSegment() {
    // A partition whose entry lists no segment holds no rows, so its class is dropped even though the pruners proved
    // nothing about any segment -- emptiness the planner can see for itself, not a pruning verdict. It is reported as
    // such: numSegmentsPrunedByBroker stays 0 because no segment was pruned.
    //
    // The broker does not publish this shape today (a partition's entry is created together with its first segment),
    // so this pins the behaviour rather than describing something reachable. It is what the golden physical plans
    // record for a leaf whose fixture builds partitions this way.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(4, false).partitionsWithoutSegments(Set.of(2, 3))
            .survivingSegments(List.of("a_seg0", "a_seg1")),
        new ColocatedTableSpec(4, false).partitionsWithoutSegments(Set.of(2, 3))
            .survivingSegments(List.of("b_seg0", "b_seg1")));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; " + colocatedJoinQuery(4) + " WHERE " + COLOCATED_TABLE_A + ".col2 = 'foo' AND "
            + COLOCATED_TABLE_B + ".col2 = 'bar'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      DispatchablePlanFragment leafA = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A);
      assertEquals(workerIdToPartitions(leafA, "a_seg"), Map.of(0, Set.of(0), 1, Set.of(1)));
      assertEquals(leafA.getWorkerIdToSegmentsMap().keySet(), Set.of(0, 1));
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 0);
    }
  }

  @Test
  public void testBrokerPruningColocatedJoinAllPrunedKeepsEveryPopulatedClass() {
    // Both sides prune everything, so the filtered union is empty and the group falls back to its populated classes.
    // A group reduced to zero workers would leave a 1-to-1 exchange with no worker to wire on either side, and the
    // server-side filter returns the same empty result from the unreduced plan anyway. So the most selective query
    // gets the least reduction, which is the same trade the leaf-level fallback makes.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(4, false).survivingSegments(List.of()),
        new ColocatedTableSpec(4, false).survivingSegments(List.of()));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; " + colocatedJoinQuery(4) + " WHERE " + COLOCATED_TABLE_A + ".col2 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertEquals(leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A).getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B).getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 0);
    }
  }

  @Test
  public void testBrokerPruningColocatedJoinDisabledByQueryOption() {
    // The existing useBrokerPruning switch is the kill switch for this too: with it off no routing query is built, so
    // nothing is provably pruned and every populated class keeps its worker.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(4, false).survivingSegments(List.of("a_seg1")),
        new ColocatedTableSpec(4, false).survivingSegments(List.of("b_seg1")));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=false; " + colocatedJoinQuery(4) + " WHERE " + COLOCATED_TABLE_A + ".col2 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertEquals(leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A).getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B).getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 0);
    }
  }

  @Test
  public void testBrokerPruningColocatedJoinFallsBackOnRoutingFailure() {
    // Pruning is best-effort on this path too: a routing call that throws must leave the group with every populated
    // class rather than fail a query that would otherwise plan.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(4, false).survivingSegments(List.of("a_seg1")),
        new ColocatedTableSpec(4, false).survivingSegments(List.of("b_seg1")), TableType.OFFLINE, true);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; " + colocatedJoinQuery(4) + " WHERE " + COLOCATED_TABLE_A + ".col2 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      assertEquals(leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A).getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B).getWorkerIdToSegmentsMap().size(), 4);
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 0);
    }
  }

  @Test
  public void testBrokerPruningColocatedJoinKeepsClassWithUnavailableSegment() {
    // An unavailable segment was selected and not pruned, so the broker cannot prove its class empty. Class 2 keeps
    // its worker on the strength of a_seg2 being unavailable rather than eliminated; only class 3, which both sides
    // prune, is dropped. Reading "absent from the routing table" as "pruned" instead would silently drop the rows a
    // transient outage hid.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(4, false).survivingSegments(List.of("a_seg0", "a_seg1"))
            .unavailableSegments(List.of("a_seg2")),
        new ColocatedTableSpec(4, false).survivingSegments(List.of("b_seg0", "b_seg1")));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; " + colocatedJoinQuery(4) + " WHERE " + COLOCATED_TABLE_A + ".col2 = 'foo' AND "
            + COLOCATED_TABLE_B + ".col2 = 'bar'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      DispatchablePlanFragment leafA = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A);
      DispatchablePlanFragment leafB = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B);
      assertEquals(workerIdToPartitions(leafA, "a_seg"), Map.of(0, Set.of(0), 1, Set.of(1), 2, Set.of(2)));
      assertEquals(workerIdToPartitions(leafB, "b_seg"), Map.of(0, Set.of(0), 1, Set.of(1), 2, Set.of(2)));
      // Only class 3 dropped: one segment on each side.
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 2);
    }
  }

  @Test
  public void testBrokerPruningColocatedJoinStillPadsAMemberWithNoDataInASurvivingClass() {
    // Emptiness padding and filter reduction have to compose. Table A holds nothing in class 3 while B does, and B's
    // filter keeps class 3, so the class survives and A gets an empty worker for it -- placed on the server B picks
    // for that class, so the exchange stays in process. Classes 1 and 2 are dropped because BOTH sides prune them.
    //
    // Padding is decided from unfiltered presence on purpose: a member that holds data the filter excludes dispatches
    // it rather than being padded, so a surviving class always has the same segment list it would have unpruned.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(4, false).emptyPartitions(Set.of(3)).survivingSegments(List.of("a_seg0")),
        new ColocatedTableSpec(4, false).survivingSegments(List.of("b_seg0", "b_seg3")));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(
        "SET useBrokerPruning=true; " + colocatedJoinQuery(4) + " WHERE " + COLOCATED_TABLE_A + ".col2 = 'foo' AND "
            + COLOCATED_TABLE_B + ".col2 = 'bar'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      DispatchablePlanFragment leafA = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A);
      DispatchablePlanFragment leafB = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B);
      // Class list [0, 3]: worker 1 of A is the padded one, so it has an entry with no segment in it.
      assertEquals(leafA.getWorkerIdToSegmentsMap().keySet(), Set.of(0, 1));
      assertEquals(workerIdToPartitions(leafA, "a_seg"), Map.of(0, Set.of(0)));
      assertEquals(workerIdToPartitions(leafB, "b_seg"), Map.of(0, Set.of(0), 1, Set.of(3)));
      Map<Integer, Integer> workerIdToClass = new HashMap<>();
      mergeWorkerIdToClass(workerIdToClass, leafA, "a_seg", 4);
      mergeWorkerIdToClass(workerIdToClass, leafB, "b_seg", 4);
      assertEquals(workerIdToClass, Map.of(0, 0, 1, 3));
      // Worker 0 stands on the same server on both sides, which is what the 1-to-1 exchange is for.
      assertEquals(workerIdToServer(leafA).get(0), workerIdToServer(leafB).get(0));
      // Worker 1 is A's padded one. It would rather land on B's server for class 3 to keep the exchange in process,
      // but here A holds no data on that server at all -- partition 3 lives only on server 4 and A is empty there --
      // and a server with no data manager for the table fails the query outright, so it falls back to a server that
      // provably hosts A and accepts one cross-server send.
      assertEquals(workerIdToServer(leafB).get(1), getServerInstance("localhost", 4).getInstanceId());
      assertTrue(Set.of(getServerInstance("localhost", 1).getInstanceId(),
              getServerInstance("localhost", 2).getInstanceId(), getServerInstance("localhost", 3).getInstanceId())
          .contains(workerIdToServer(leafA).get(1)), workerIdToServer(leafA).toString());
      // Classes 1 and 2 dropped, one segment each on each side.
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 4);
    }
  }

  @Test
  public void testColocatedJoinReducedGroupPrunesWholeClasses() {
    // Emptiness reduction and filter reduction compose, on the one shape whose partitioned leaf is not marked
    // pre-partitioned: a fact table joined with a replicated dimension table over an explicit local exchange.
    //
    // 8 partitions over 4 classes, so class c holds partitions c and c+4. The fact table's class 3 is empty, and the
    // filter leaves only a_seg0 and a_seg4, which are both class 0. Emptiness drops class 3, the filter drops classes
    // 1 and 2, and the class list ends up [0] -- a single worker holding the whole of class 0.
    //
    // This is the shape that used to be gated: reducing to [0] by dropping classes from the SHARED list keeps the
    // worker id equal to its index in that list. Compacting at the leaf instead -- which is what
    // assignMultiplePartitionsPerWorker would do if a per-leaf verdict ever reached it -- would leave the leaf one
    // worker while its class list still claimed three.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(8, false).emptyPartitions(Set.of(3, 7)).survivingSegments(List.of("a_seg0", "a_seg4")),
        new ColocatedTableSpec(8, false));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile("SET useBrokerPruning=true; "
        + replicatedDimensionJoinQuery(4) + " WHERE " + COLOCATED_TABLE_A + ".col2 = 'foo'")) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      // Classes 1 and 2, two segments each. Class 3 is empty, not pruned, so it is not counted.
      assertEquals(dispatchableSubPlan.getNumSegmentsPrunedByBroker(), 4);
      DispatchablePlanFragment leafA = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A);
      DispatchablePlanFragment leafB = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B);
      // One worker for the one surviving class, holding both of its partitions, and no worker dropped or padded.
      assertEquals(workerIdToPartitions(leafA, "a_seg"), Map.of(0, Set.of(0, 4)));
      assertEquals(leafA.getWorkerIdToSegmentsMap().keySet(), Set.of(0));
      Map<Integer, Integer> workerIdToClass = new HashMap<>();
      mergeWorkerIdToClass(workerIdToClass, leafA, "a_seg", 4);
      assertEquals(workerIdToClass, Map.of(0, 0));
      // The replicated leaf and the join derive their workers from the fact leaf, so they follow it class for class.
      assertEquals(leafB.getWorkerIdToSegmentsMap().keySet(), leafA.getWorkerIdToSegmentsMap().keySet());
      assertEquals(workerIdToServer(leafB), workerIdToServer(leafA));
      assertEquals(joinFragment(dispatchableSubPlan).getWorkerMetadataList().size(), 1);
    }
  }

  @Test
  public void testColocatedJoinReducedGroupWithReplicatedLeaf() {
    // The most common colocated shape: a partitioned fact table joined with a replicated dimension table over a local
    // exchange. The fact table is the group's only source of classes -- a replicated one says nothing about which of
    // them hold data (see LeafPartitionHints#isReplicated) -- so its empty class 3 is dropped rather than padded.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(Set.of(3), Set.of(), Set.of(), Set.of(), true);
    try (QueryEnvironment.CompiledQuery compiledQuery =
        queryEnvironment.compile(replicatedDimensionJoinQuery(4))) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      DispatchablePlanFragment leafA = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A);
      DispatchablePlanFragment leafB = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B);
      // The fact leaf keeps one worker per surviving class and pads nothing.
      assertEquals(workerIdToPartitions(leafA, "a_seg"), Map.of(0, Set.of(0), 1, Set.of(1), 2, Set.of(2)));
      assertEquals(leafA.getWorkerIdToSegmentsMap().keySet(), Set.of(0, 1, 2));
      // The replicated leaf follows the reduced fact leaf: same worker ids, each scanning the whole dimension table on
      // the same server as its fact-table peer.
      assertEquals(leafB.getWorkerIdToSegmentsMap().keySet(), leafA.getWorkerIdToSegmentsMap().keySet());
      for (Integer workerId : leafB.getWorkerIdToSegmentsMap().keySet()) {
        assertEquals(new HashSet<>(assignedSegments(leafB, workerId)),
            Set.of("b_seg0", "b_seg1", "b_seg2", "b_seg3"));
      }
      assertEquals(workerIdToServer(leafB), workerIdToServer(leafA));
      // The join keeps the same 3 workers on the same servers, so both exchanges into it stay 1-to-1 and in process:
      // each join worker reads a single local mailbox holding its own worker id from each side.
      DispatchablePlanFragment joinFragment = joinFragment(dispatchableSubPlan);
      assertEquals(joinFragment.getWorkerMetadataList().size(), 3);
      assertEquals(workerIdToServer(joinFragment), workerIdToServer(leafA));
      int leafAFragmentId = leafA.getPlanFragment().getFragmentId();
      int leafBFragmentId = leafB.getPlanFragment().getFragmentId();
      for (int workerId = 0; workerId < 3; workerId++) {
        Map<Integer, MailboxInfos> mailboxInfosMap =
            joinFragment.getWorkerMetadataList().get(workerId).getMailboxInfosMap();
        for (int senderFragmentId : List.of(leafAFragmentId, leafBFragmentId)) {
          MailboxInfos mailboxInfos = mailboxInfosMap.get(senderFragmentId);
          assertNotNull(mailboxInfos, "No mailbox for sender: " + senderFragmentId + " on worker: " + workerId);
          assertTrue(mailboxInfos instanceof SharedMailboxInfos, String.valueOf(mailboxInfos));
          assertEquals(mailboxInfos.getMailboxInfos().size(), 1);
          assertEquals(mailboxInfos.getMailboxInfos().get(0).getWorkerIds(), List.of(workerId));
        }
      }
    }
  }

  @Test
  public void testColocatedJoinReducedGroupWithPartitionParallelism() {
    // With partition_parallelism = p the leaf still gets one worker per surviving class while the stage reading it gets
    // p workers per sender, i.e. join worker k handles the class at index k / p. Class 3 is empty on both sides, so
    // that arithmetic runs over the reduced list [0, 1, 2] rather than over 0..partitionSize-1.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(Set.of(3), Set.of(3), Set.of(), Set.of());
    String tableHint = colocatedTableHint(4, 2);
    try (QueryEnvironment.CompiledQuery compiledQuery =
        queryEnvironment.compile(colocatedJoinQuery(tableHint, tableHint))) {
      DispatchableSubPlan dispatchableSubPlan = compiledQuery.planQuery(0).getQueryPlan();
      DispatchablePlanFragment leafA = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_A);
      DispatchablePlanFragment leafB = leafFragmentForTable(dispatchableSubPlan, COLOCATED_TABLE_B);
      assertEquals(workerIdToPartitions(leafA, "a_seg"), Map.of(0, Set.of(0), 1, Set.of(1), 2, Set.of(2)));
      assertEquals(workerIdToPartitions(leafB, "b_seg"), Map.of(0, Set.of(0), 1, Set.of(1), 2, Set.of(2)));
      // 3 surviving classes x parallelism 2.
      DispatchablePlanFragment joinFragment = joinFragment(dispatchableSubPlan);
      assertEquals(joinFragment.getWorkerMetadataList().size(), 6);
      int leafAFragmentId = leafA.getPlanFragment().getFragmentId();
      int joinFragmentId = joinFragment.getPlanFragment().getFragmentId();
      // Receiver k reads sender k / 2, and runs on that sender's server. The two receivers of a sender share its single
      // local mailbox, hence SharedMailboxInfos.
      for (int workerId = 0; workerId < 6; workerId++) {
        MailboxInfos mailboxInfos =
            joinFragment.getWorkerMetadataList().get(workerId).getMailboxInfosMap().get(leafAFragmentId);
        assertNotNull(mailboxInfos, "No mailbox for table A's leaf on worker: " + workerId);
        assertTrue(mailboxInfos instanceof SharedMailboxInfos, String.valueOf(mailboxInfos));
        assertEquals(mailboxInfos.getMailboxInfos().size(), 1);
        assertEquals(mailboxInfos.getMailboxInfos().get(0).getWorkerIds(), List.of(workerId / 2));
        assertEquals(workerIdToServer(joinFragment).get(workerId), workerIdToServer(leafA).get(workerId / 2));
      }
      // And the other way round: sender k fans out to the contiguous receiver range [2k, 2k + 1].
      for (int workerId = 0; workerId < 3; workerId++) {
        MailboxInfos mailboxInfos =
            leafA.getWorkerMetadataList().get(workerId).getMailboxInfosMap().get(joinFragmentId);
        assertNotNull(mailboxInfos, "No mailbox for the join stage on worker: " + workerId);
        assertEquals(mailboxInfos.getMailboxInfos().size(), 1);
        assertEquals(mailboxInfos.getMailboxInfos().get(0).getWorkerIds(), List.of(2 * workerId, 2 * workerId + 1));
      }
    }
  }

  @Test
  public void testPartitionedLeafRejectsPartitionWithOnlyDeferredSegmentsWithMultiplePartitionsPerWorker() {
    // Worker 3 covers the partition class {3, 7}, where partition 3 is genuinely empty but partition 7 has no entry
    // only because all of its segments are deferred, so the class cannot be padded: that would drop partition 7's rows.
    QueryEnvironment queryEnvironment =
        newColocatedJoinQueryEnvironment(Set.of(3, 7), Set.of(), Set.of(7), Set.of(), true, 8);
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_JOIN_QUERY)) {
      RuntimeException e = expectThrows(RuntimeException.class, () -> compiledQuery.planQuery(0));
      Throwable cause = e.getCause();
      assertTrue(cause instanceof IllegalStateException, String.valueOf(cause));
      assertTrue(cause.getMessage().contains("Failed to find a fully replicated server for partitions: [7]"),
          cause.getMessage());
      assertTrue(cause.getMessage().contains("of table: " + COLOCATED_TABLE_A), cause.getMessage());
    }
  }

  @Test
  public void testColocatedJoinRejectsSegmentsWithInvalidPartition() {
    // Segments with invalid partition metadata are absent from the partition info map altogether, and unlike an empty
    // partition there is nothing to pad: their rows may belong to any partition.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(4, true).segmentsWithInvalidPartition(List.of("a_segBad")),
        new ColocatedTableSpec(4, true));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_JOIN_QUERY)) {
      RuntimeException e = expectThrows(RuntimeException.class, () -> compiledQuery.planQuery(0));
      Throwable cause = e.getCause();
      assertTrue(cause instanceof IllegalStateException, String.valueOf(cause));
      assertTrue(cause.getMessage().contains("1 segments with invalid partition for table: "
          + COLOCATED_TABLE_A_OFFLINE), cause.getMessage());
    }
  }

  @Test
  public void testColocatedJoinRejectsPartitionWithoutFullyReplicatedServer() {
    // Partition 2 of table A holds a segment, but no single server holds the whole partition. There is an entry, so
    // nothing to pad, and it must keep failing at the server-pick precondition instead of being reduced away.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(4, false).partitionsWithoutFullyReplicatedServer(Set.of(2)),
        new ColocatedTableSpec(4, false));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_JOIN_QUERY)) {
      RuntimeException e = expectThrows(RuntimeException.class, () -> compiledQuery.planQuery(0));
      Throwable cause = e.getCause();
      assertTrue(cause instanceof IllegalStateException, String.valueOf(cause));
      assertTrue(cause.getMessage().contains("Failed to find enabled fully replicated server for table: "
          + COLOCATED_TABLE_A), cause.getMessage());
      assertTrue(cause.getMessage().contains("partition: 2"), cause.getMessage());
    }
  }

  @Test
  public void testColocatedJoinRejectsClassWithoutFullyReplicatedServerWithMultiplePartitionsPerWorker() {
    // Same, on the several-partitions-per-worker path: worker 3 covers {3, 7}, where partition 3 is empty and partition
    // 7 has no fully replicated server. The class holds data, so it is not padded, and no server can scan it whole.
    QueryEnvironment queryEnvironment = newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(8, false).emptyPartitions(Set.of(3))
            .partitionsWithoutFullyReplicatedServer(Set.of(7)),
        new ColocatedTableSpec(8, false));
    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(COLOCATED_JOIN_QUERY)) {
      RuntimeException e = expectThrows(RuntimeException.class, () -> compiledQuery.planQuery(0));
      Throwable cause = e.getCause();
      assertTrue(cause instanceof IllegalStateException, String.valueOf(cause));
      assertTrue(cause.getMessage().contains("Failed to find enabled fully replicated server for table: "
          + COLOCATED_TABLE_A), cause.getMessage());
      assertTrue(cause.getMessage().contains("partition class: 3"), cause.getMessage());
    }
  }

  // ---------------------------------------------------------------------------
  // Partitioned leaf assignment shape invariants
  // ---------------------------------------------------------------------------

  @Test
  public void testCheckLeafWorkerAssignmentRejectsSparseWorkerIds() {
    // DispatchablePlanContext sizes a WorkerMetadata[] from the server map and indexes it by worker id, so a gap would
    // leave a null entry (and an out-of-range id would throw an ArrayIndexOutOfBoundsException) there instead of here.
    Map<Integer, QueryServerInstance> serverMap = Map.of(0, queryServerInstance(1), 2, queryServerInstance(2));
    Map<Integer, Map<String, List<String>>> segmentsMap =
        Map.of(0, offlineSegments("seg0"), 2, offlineSegments("seg2"));
    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> WorkerManager.checkLeafWorkerAssignment("testTable", null, serverMap, segmentsMap));
    assertTrue(e.getMessage().contains("Missing server instance for worker: 1"), e.getMessage());
  }

  @Test
  public void testCheckLeafWorkerAssignmentRejectsKeySetMismatch() {
    Map<Integer, QueryServerInstance> serverMap = Map.of(0, queryServerInstance(1), 1, queryServerInstance(2));
    Map<Integer, Map<String, List<String>>> segmentsMap =
        Map.of(0, offlineSegments("seg0"), 5, offlineSegments("seg5"));
    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> WorkerManager.checkLeafWorkerAssignment("testTable", null, serverMap, segmentsMap));
    assertTrue(e.getMessage().contains("Missing segments for worker: 1"), e.getMessage());
  }

  @Test
  public void testCheckLeafWorkerAssignmentRejectsNullSegmentList() {
    Map<String, List<String>> nullList = new HashMap<>();
    nullList.put(TableType.OFFLINE.name(), null);
    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> WorkerManager.checkLeafWorkerAssignment("testTable", null, Map.of(0, queryServerInstance(1)),
            Map.of(0, nullList)));
    assertTrue(e.getMessage().contains("Null segment list for table type: OFFLINE"), e.getMessage());
  }

  @Test
  public void testCheckLeafWorkerAssignmentRejectsEmptyTableTypeMap() {
    // The server splits the request on the number of entries in this map, so a worker with no table type at all would
    // produce no server request.
    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> WorkerManager.checkLeafWorkerAssignment("testTable", null, Map.of(0, queryServerInstance(1)),
            Map.of(0, Map.of())));
    assertTrue(e.getMessage().contains("Expected 1 or 2 table types for worker: 0, got: 0"), e.getMessage());
  }

  @Test
  public void testCheckLeafWorkerAssignmentRejectsThreeTableTypeMap() {
    Map<String, List<String>> threeTypes = new HashMap<>();
    threeTypes.put(TableType.OFFLINE.name(), List.of());
    threeTypes.put(TableType.REALTIME.name(), List.of());
    threeTypes.put("HYBRID", List.of());
    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> WorkerManager.checkLeafWorkerAssignment("testTable", null, Map.of(0, queryServerInstance(1)),
            Map.of(0, threeTypes)));
    assertTrue(e.getMessage().contains("Expected 1 or 2 table types for worker: 0, got: 3"), e.getMessage());
  }

  @Test
  public void testCheckLeafWorkerAssignmentRejectsUnknownTableType() {
    // The server resolves one table data manager per key in this map, and reports a missing table for an unknown one.
    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> WorkerManager.checkLeafWorkerAssignment("testTable", null, Map.of(0, queryServerInstance(1)),
            Map.of(0, Map.of("HYBRID", List.of()))));
    assertTrue(e.getMessage().contains("Unexpected table type: HYBRID for worker: 0"), e.getMessage());
  }

  @Test
  public void testCheckLeafWorkerAssignmentRejectsFewerWorkersThanPartitionClasses() {
    // A worker id of a colocated leaf IS an index into the group's shared class list, so a member that assigned fewer
    // workers than the list has renumbered every class after the gap. checkPartitionClassAgreement cannot see it: it
    // compares the shared array against itself, and both sides still hold the same instance.
    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> WorkerManager.checkLeafWorkerAssignment("testTable", new int[]{0, 2, 5},
            Map.of(0, queryServerInstance(1), 1, queryServerInstance(2)),
            Map.of(0, offlineSegments("seg0"), 1, offlineSegments("seg2"))));
    assertTrue(e.getMessage().contains("Got 2 workers for partition classes: [0, 2, 5]"), e.getMessage());
  }

  @Test
  public void testCheckLeafWorkerAssignmentAcceptsOneWorkerPerPartitionClass() {
    WorkerManager.checkLeafWorkerAssignment("testTable", new int[]{0, 2},
        Map.of(0, queryServerInstance(1), 1, queryServerInstance(2)),
        Map.of(0, offlineSegments("seg0"), 1, offlineSegments("seg2")));
  }

  @Test
  public void testCheckLeafWorkerAssignmentAcceptsHybridAndEmptySegmentWorkers() {
    // The two shapes the partitioned assignment produces: a hybrid worker with both table types, and one with a single
    // table type mapped to an empty list.
    Map<String, List<String>> hybridSegments = new HashMap<>();
    hybridSegments.put(TableType.OFFLINE.name(), List.of("segO0"));
    hybridSegments.put(TableType.REALTIME.name(), List.of("segR0"));
    WorkerManager.checkLeafWorkerAssignment("testTable", null,
        Map.of(0, queryServerInstance(1), 1, queryServerInstance(2)),
        Map.of(0, hybridSegments, 1, Map.of(TableType.OFFLINE.name(), new ArrayList<>())));
  }

  private static QueryServerInstance queryServerInstance(int port) {
    return new QueryServerInstance(getServerInstance("localhost", port));
  }

  private static Map<String, List<String>> offlineSegments(String... segments) {
    return Map.of(TableType.OFFLINE.name(), List.of(segments));
  }

  private static final String COLOCATED_TABLE_A = "tableA";
  private static final String COLOCATED_TABLE_A_OFFLINE = "tableA_OFFLINE";
  private static final String COLOCATED_TABLE_B = "tableB";
  private static final String COLOCATED_TABLE_B_OFFLINE = "tableB_OFFLINE";
  private static final String COLOCATED_TABLE_HINT = colocatedTableHint(4);
  private static final String COLOCATED_JOIN_QUERY = colocatedJoinQuery(4);
  private static final String COLOCATED_NON_EQUI_JOIN_QUERY =
      "SELECT /*+ joinOptions(is_colocated_by_join_keys='true') */ " + COLOCATED_TABLE_A + ".col2, "
          + COLOCATED_TABLE_B + ".col2 FROM " + COLOCATED_TABLE_A + " " + COLOCATED_TABLE_HINT + "JOIN "
          + COLOCATED_TABLE_B + " " + COLOCATED_TABLE_HINT + "ON " + COLOCATED_TABLE_A + ".col3 < "
          + COLOCATED_TABLE_B + ".col3";

  private static String colocatedTableHint(int partitionSize) {
    return "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='" + partitionSize
        + "') */ ";
  }

  /// Same as [#colocatedTableHint(int)], with an explicit `partition_parallelism`.
  private static String colocatedTableHint(int partitionSize, int partitionParallelism) {
    return "/*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='" + partitionSize
        + "', partition_parallelism='" + partitionParallelism + "') */ ";
  }

  /// A colocated (equi) join of "tableA" and "tableB", both hinted with the given `partition_size`.
  private static String colocatedJoinQuery(int partitionSize) {
    String tableHint = colocatedTableHint(partitionSize);
    return colocatedJoinQuery(tableHint, tableHint);
  }

  /// Same as [#colocatedJoinQuery(int)], with the table hint of each side given explicitly so that the two sides can
  /// differ (e.g. an explicit partition parallelism on both).
  private static String colocatedJoinQuery(String tableHintA, String tableHintB) {
    return "SELECT /*+ joinOptions(is_colocated_by_join_keys='true') */ " + COLOCATED_TABLE_A + ".col2, "
        + COLOCATED_TABLE_B + ".col2 FROM " + COLOCATED_TABLE_A + " " + tableHintA + "JOIN " + COLOCATED_TABLE_B + " "
        + tableHintB + "ON " + COLOCATED_TABLE_A + ".col1 = " + COLOCATED_TABLE_B + ".col1";
  }

  /// Same as [#colocatedJoinQuery(int)], joining on "col2" while both tables are hinted partitioned on "col1". This is
  /// `is_colocated_by_join_keys` in its intended form: the user asserts that the join key is laid out the same way as
  /// the hinted partition key, which Pinot has no metadata to verify.
  private static String colocatedJoinQueryOnNonPartitionKey(int partitionSize) {
    String tableHint = colocatedTableHint(partitionSize);
    return "SELECT /*+ joinOptions(is_colocated_by_join_keys='true') */ " + COLOCATED_TABLE_A + ".col1, "
        + COLOCATED_TABLE_B + ".col1 FROM " + COLOCATED_TABLE_A + " " + tableHint + "JOIN " + COLOCATED_TABLE_B + " "
        + tableHint + "ON " + COLOCATED_TABLE_A + ".col2 = " + COLOCATED_TABLE_B + ".col2";
  }

  /// A join of the partitioned fact table "tableA" with "tableB" hinted replicated, both sides sent over a local
  /// exchange. This is the shape [#colocatedJoinQuery(int)] cannot express: `is_colocated_by_join_keys` claims both
  /// sides are partitioned by the join key, while a replicated table is simply present in full on every worker.
  private static String replicatedDimensionJoinQuery(int partitionSize) {
    return "SELECT /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */ "
        + COLOCATED_TABLE_A + ".col2, " + COLOCATED_TABLE_B + ".col2 FROM " + COLOCATED_TABLE_A + " "
        + colocatedTableHint(partitionSize) + "JOIN " + COLOCATED_TABLE_B
        + " /*+ tableOptions(is_replicated='true') */ ON " + COLOCATED_TABLE_A + ".col1 = " + COLOCATED_TABLE_B
        + ".col1";
  }

  /// Builds a QueryEnvironment for two offline partitioned tables "tableA" and "tableB" (function Hashcode on col1, 4
  /// partitions each), for colocated join tests. Partition `p` of table `t` holds one segment `"{t}_seg{p}"` fully
  /// replicated on server `p`, unless `p` is in that table's `emptyPartitions`, in which case it has no entry in the
  /// partition info map at all. The `deferredPartitions` are the ones the broker reports as absent only because all of
  /// their segments are new and not fully online yet.
  private static QueryEnvironment newColocatedJoinQueryEnvironment(Set<Integer> emptyPartitionsA,
      Set<Integer> emptyPartitionsB, Set<Integer> deferredPartitionsA, Set<Integer> deferredPartitionsB) {
    return newColocatedJoinQueryEnvironment(emptyPartitionsA, emptyPartitionsB, deferredPartitionsA,
        deferredPartitionsB, false);
  }

  /// Same as [#newColocatedJoinQueryEnvironment(Set, Set, Set, Set)], except that when
  /// `everyServerHostsEveryPartition` is set each partition is fully replicated on all the servers instead of only on
  /// its own one.
  private static QueryEnvironment newColocatedJoinQueryEnvironment(Set<Integer> emptyPartitionsA,
      Set<Integer> emptyPartitionsB, Set<Integer> deferredPartitionsA, Set<Integer> deferredPartitionsB,
      boolean everyServerHostsEveryPartition) {
    return newColocatedJoinQueryEnvironment(emptyPartitionsA, emptyPartitionsB, deferredPartitionsA,
        deferredPartitionsB, everyServerHostsEveryPartition, 4);
  }

  /// Same as [#newColocatedJoinQueryEnvironment(Set, Set, Set, Set, boolean)], with the number of partitions of each
  /// table. There are always 4 servers, so with more partitions than that, partition `p` lives on server `p % 4` and
  /// several partitions share a worker.
  private static QueryEnvironment newColocatedJoinQueryEnvironment(Set<Integer> emptyPartitionsA,
      Set<Integer> emptyPartitionsB, Set<Integer> deferredPartitionsA, Set<Integer> deferredPartitionsB,
      boolean everyServerHostsEveryPartition, int numPartitionsPerTable) {
    return newColocatedJoinQueryEnvironment(
        new ColocatedTableSpec(numPartitionsPerTable, everyServerHostsEveryPartition).emptyPartitions(emptyPartitionsA)
            .partitionsWithOnlyDeferredSegments(deferredPartitionsA),
        new ColocatedTableSpec(numPartitionsPerTable, everyServerHostsEveryPartition).emptyPartitions(emptyPartitionsB)
            .partitionsWithOnlyDeferredSegments(deferredPartitionsB));
  }

  /// Same as [#newColocatedJoinQueryEnvironment(Set, Set, Set, Set, boolean, int)], taking the full layout of each
  /// table so that a test can also make a partition unservable or give it invalid partition metadata.
  private static QueryEnvironment newColocatedJoinQueryEnvironment(ColocatedTableSpec specA,
      ColocatedTableSpec specB) {
    return newColocatedJoinQueryEnvironment(specA, specB, TableType.OFFLINE);
  }

  /// Same as [#newColocatedJoinQueryEnvironment(ColocatedTableSpec, ColocatedTableSpec)], with the table type both
  /// tables are registered under, so that the realtime-only shape can be covered too.
  private static QueryEnvironment newColocatedJoinQueryEnvironment(ColocatedTableSpec specA, ColocatedTableSpec specB,
      TableType tableType) {
    return newColocatedJoinQueryEnvironment(specA, specB, tableType, false);
  }

  /// Same as [#newColocatedJoinQueryEnvironment(ColocatedTableSpec, ColocatedTableSpec, TableType)], with a routing
  /// manager that throws on every routing call, to exercise the best-effort fallback.
  private static QueryEnvironment newColocatedJoinQueryEnvironment(ColocatedTableSpec specA, ColocatedTableSpec specB,
      TableType tableType, boolean throwOnRouting) {
    int numServers = 4;
    ServerInstance[] servers = new ServerInstance[numServers];
    Map<String, ServerInstance> enabledServers = new HashMap<>();
    for (int i = 0; i < numServers; i++) {
      servers[i] = getServerInstance("localhost", i + 1);
      enabledServers.put(servers[i].getInstanceId(), servers[i]);
    }
    String tableAWithType = COLOCATED_TABLE_A + "_" + tableType.name();
    String tableBWithType = COLOCATED_TABLE_B + "_" + tableType.name();
    Map<String, TablePartitionReplicatedServersInfo> partitionInfoByTable = new HashMap<>();
    partitionInfoByTable.put(tableAWithType,
        colocatedTablePartitionInfo(tableAWithType, "a_seg", servers, specA));
    partitionInfoByTable.put(tableBWithType,
        colocatedTablePartitionInfo(tableBWithType, "b_seg", servers, specB));
    Map<String, RoutingTable> routingTableByTable = new HashMap<>();
    if (specA._survivingSegments != null) {
      routingTableByTable.put(tableAWithType,
          colocatedRoutingTable(servers, "a_seg", specA._survivingSegments, specA._unavailableSegments));
    }
    if (specB._survivingSegments != null) {
      routingTableByTable.put(tableBWithType,
          colocatedRoutingTable(servers, "b_seg", specB._survivingSegments, specB._unavailableSegments));
    }
    PartitionedRoutingManager routingManager =
        new PartitionedRoutingManager(enabledServers, partitionInfoByTable, routingTableByTable, throwOnRouting);

    Map<String, String> tableNameMap = new HashMap<>();
    tableNameMap.put(tableAWithType, tableAWithType);
    tableNameMap.put(COLOCATED_TABLE_A, COLOCATED_TABLE_A);
    tableNameMap.put(tableBWithType, tableBWithType);
    tableNameMap.put(COLOCATED_TABLE_B, COLOCATED_TABLE_B);
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableNameMap()).thenReturn(tableNameMap);
    when(tableCache.getActualTableName(anyString())).thenAnswer(inv -> tableNameMap.get(inv.getArgument(0)));
    when(tableCache.getSchema(anyString())).thenAnswer(
        inv -> getSchemaBuilder(inv.getArgument(0, String.class)).build());
    when(tableCache.getTableConfig(anyString())).thenReturn(mock(TableConfig.class));

    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 5, routingManager);
    return new QueryEnvironment(QueryEnvironment.configBuilder()
        .requestId(-1L)
        .database(CommonConstants.DEFAULT_DATABASE)
        .tableCache(tableCache)
        .workerManager(workerManager)
        .build());
  }

  private static TablePartitionReplicatedServersInfo colocatedTablePartitionInfo(String tableNameWithType,
      String segmentPrefix, ServerInstance[] servers, ColocatedTableSpec spec) {
    Set<String> allServers = new HashSet<>();
    for (ServerInstance server : servers) {
      allServers.add(server.getInstanceId());
    }
    int numPartitions = spec._numPartitions;
    TablePartitionReplicatedServersInfo.PartitionInfo[] partitionInfoMap =
        new TablePartitionReplicatedServersInfo.PartitionInfo[numPartitions];
    for (int p = 0; p < numPartitions; p++) {
      if (!spec._emptyPartitions.contains(p)) {
        Set<String> partitionServers;
        if (spec._partitionsWithoutFullyReplicatedServer.contains(p)) {
          // The partition holds a segment, but no single server holds all of it.
          partitionServers = Set.of();
        } else if (spec._partitionServerIndexes.containsKey(p)) {
          partitionServers = new HashSet<>();
          for (Integer serverIndex : spec._partitionServerIndexes.get(p)) {
            partitionServers.add(servers[serverIndex].getInstanceId());
          }
        } else {
          partitionServers = spec._everyServerHostsEveryPartition ? allServers
              : Set.of(servers[p % servers.length].getInstanceId());
        }
        // Mutable, like the lists the broker publishes: the assignment must hand out a copy rather than this instance.
        List<String> segments = spec._partitionsWithoutSegments.contains(p) ? new ArrayList<>()
            : new ArrayList<>(List.of(segmentPrefix + p));
        partitionInfoMap[p] = new TablePartitionReplicatedServersInfo.PartitionInfo(partitionServers, segments);
      }
    }
    return new TablePartitionReplicatedServersInfo(tableNameWithType, "col1", "Hashcode", numPartitions,
        partitionInfoMap, spec._segmentsWithInvalidPartition, spec._partitionsWithOnlyDeferredSegments);
  }

  /// Buckets the given surviving segments onto the server hosting their partition (partition `p` lives on server
  /// `p % 4`), i.e. builds what the routing manager returns for one colocated table's filtered routing query.
  private static RoutingTable colocatedRoutingTable(ServerInstance[] servers, String segmentPrefix,
      List<String> survivingSegments, List<String> unavailableSegments) {
    Map<ServerInstance, List<String>> serverToSegmentList = new HashMap<>();
    for (String segment : survivingSegments) {
      int partition = Integer.parseInt(segment.substring(segmentPrefix.length()));
      serverToSegmentList.computeIfAbsent(servers[partition % servers.length], k -> new ArrayList<>()).add(segment);
    }
    Map<ServerInstance, SegmentsToQuery> serverToSegments = new HashMap<>();
    serverToSegmentList.forEach((server, segments) -> serverToSegments.put(server,
        new SegmentsToQuery(segments, List.of())));
    return new RoutingTable(serverToSegments, new ArrayList<>(unavailableSegments), 0);
  }

  /// How one side of a colocated join is laid out, for [#newColocatedJoinQueryEnvironment(ColocatedTableSpec,
  /// ColocatedTableSpec)]. Partition `p` holds one segment named after the table's prefix, fully replicated on server
  /// `p % 4` (or on all 4 servers when `everyServerHostsEveryPartition` is set), unless a set below says otherwise.
  private static class ColocatedTableSpec {
    final int _numPartitions;
    final boolean _everyServerHostsEveryPartition;
    /// Partitions with no entry at all in the partition info map, i.e. holding no segment.
    Set<Integer> _emptyPartitions = Set.of();
    /// Partitions the broker reports as having no entry only because all their segments are new and not fully online.
    Set<Integer> _partitionsWithOnlyDeferredSegments = Set.of();
    /// Partitions with an entry but no fully replicated server, i.e. holding a segment that no single server has whole.
    Set<Integer> _partitionsWithoutFullyReplicatedServer = Set.of();
    /// Segments the broker reports as holding invalid partition metadata (absent from the partition info map).
    List<String> _segmentsWithInvalidPartition = List.of();
    /// Overrides the servers of individual partitions, by server index, so that a partition's servers can be a strict
    /// subset of the ones hosting the table as a whole.
    Map<Integer, Set<Integer>> _partitionServerIndexes = Map.of();
    /// The segments the routing manager reports as surviving the filtered routing query, i.e. what broker pruning would
    /// keep. Null when the table gets no routing table at all, so that a routing call a test did not expect returns
    /// null rather than a silently empty answer.
    @Nullable
    List<String> _survivingSegments;
    List<String> _unavailableSegments = List.of();
    Set<Integer> _partitionsWithoutSegments = Set.of();

    ColocatedTableSpec(int numPartitions, boolean everyServerHostsEveryPartition) {
      _numPartitions = numPartitions;
      _everyServerHostsEveryPartition = everyServerHostsEveryPartition;
    }

    ColocatedTableSpec emptyPartitions(Set<Integer> emptyPartitions) {
      _emptyPartitions = emptyPartitions;
      return this;
    }

    ColocatedTableSpec partitionsWithOnlyDeferredSegments(Set<Integer> partitionsWithOnlyDeferredSegments) {
      _partitionsWithOnlyDeferredSegments = partitionsWithOnlyDeferredSegments;
      return this;
    }

    ColocatedTableSpec partitionsWithoutFullyReplicatedServer(Set<Integer> partitionsWithoutFullyReplicatedServer) {
      _partitionsWithoutFullyReplicatedServer = partitionsWithoutFullyReplicatedServer;
      return this;
    }

    ColocatedTableSpec segmentsWithInvalidPartition(List<String> segmentsWithInvalidPartition) {
      _segmentsWithInvalidPartition = segmentsWithInvalidPartition;
      return this;
    }

    ColocatedTableSpec partitionServerIndexes(Map<Integer, Set<Integer>> partitionServerIndexes) {
      _partitionServerIndexes = partitionServerIndexes;
      return this;
    }

    ColocatedTableSpec survivingSegments(List<String> survivingSegments) {
      _survivingSegments = survivingSegments;
      return this;
    }

    /// Partitions that get an entry listing no segment, as opposed to no entry at all. The broker never publishes
    /// this shape today -- a partition's entry is created together with its first segment -- so it exists only to pin
    /// what the assignment does if one ever appears.
    ColocatedTableSpec partitionsWithoutSegments(Set<Integer> partitionsWithoutSegments) {
      _partitionsWithoutSegments = partitionsWithoutSegments;
      return this;
    }

    /// Segments the routing table reports as unavailable. They were selected and not pruned, so the broker cannot
    /// prove them empty and their partition class has to survive.
    ColocatedTableSpec unavailableSegments(List<String> unavailableSegments) {
      _unavailableSegments = unavailableSegments;
      return this;
    }
  }

  /// Returns the only fragment below the reduce stage with neither segments nor children, i.e. the join stage.
  private static DispatchablePlanFragment joinFragment(DispatchableSubPlan dispatchableSubPlan) {
    for (Map.Entry<Integer, DispatchablePlanFragment> entry : dispatchableSubPlan.getQueryStageMap().entrySet()) {
      if (entry.getKey() != 0 && entry.getValue().getWorkerIdToSegmentsMap().isEmpty()) {
        return entry.getValue();
      }
    }
    throw new AssertionError("Found no join fragment in: " + dispatchableSubPlan.getQueryStageMap().keySet());
  }

  /// Returns the leaf fragment scanning the given table.
  private static DispatchablePlanFragment leafFragmentForTable(DispatchableSubPlan dispatchableSubPlan,
      String tableName) {
    for (DispatchablePlanFragment leafFragment : leafFragments(dispatchableSubPlan)) {
      if (leafFragment.getTableName().startsWith(tableName)) {
        return leafFragment;
      }
    }
    throw new AssertionError("Found no leaf fragment for table: " + tableName);
  }

  /// Returns every server instance id the plan is dispatched to, over all the stages but the broker reduce root.
  private static Set<String> dispatchedServers(DispatchableSubPlan dispatchableSubPlan) {
    Set<String> servers = new HashSet<>();
    for (Map.Entry<Integer, DispatchablePlanFragment> entry : dispatchableSubPlan.getQueryStageMap().entrySet()) {
      if (entry.getKey() != 0) {
        for (QueryServerInstance server : entry.getValue().getServerInstances()) {
          servers.add(server.getInstanceId());
        }
      }
    }
    return servers;
  }

  /// Maps each worker id of the given leaf to the partitions it scans, decoded from the `{segmentPrefix}{partition}`
  /// segment names. A worker with no segment is absent from the result: nothing but its index in the class list the
  /// colocated group shares says which class it stands for.
  private static Map<Integer, Set<Integer>> workerIdToPartitions(DispatchablePlanFragment leafFragment,
      String segmentPrefix) {
    Map<Integer, Set<Integer>> workerIdToPartitions = new HashMap<>();
    for (Map.Entry<Integer, Map<String, List<String>>> entry : leafFragment.getWorkerIdToSegmentsMap().entrySet()) {
      Set<Integer> partitions = new HashSet<>();
      for (List<String> segments : entry.getValue().values()) {
        for (String segment : segments) {
          assertTrue(segment.startsWith(segmentPrefix), "Unexpected segment: " + segment);
          partitions.add(Integer.parseInt(segment.substring(segmentPrefix.length())));
        }
      }
      if (!partitions.isEmpty()) {
        workerIdToPartitions.put(entry.getKey(), partitions);
      }
    }
    return workerIdToPartitions;
  }

  /// Folds the worker id -> partition class mapping of one leaf into `workerIdToClass`, failing when this leaf
  /// contradicts what another leaf of the same colocated group already recorded for a worker id. A worker with no
  /// segment contributes nothing, so the map is only filled in from the sides that hold data.
  private static void mergeWorkerIdToClass(Map<Integer, Integer> workerIdToClass,
      DispatchablePlanFragment leafFragment, String segmentPrefix, int partitionSize) {
    for (Map.Entry<Integer, Set<Integer>> entry : workerIdToPartitions(leafFragment, segmentPrefix).entrySet()) {
      Integer workerId = entry.getKey();
      Set<Integer> partitionClasses = new HashSet<>();
      for (Integer partition : entry.getValue()) {
        partitionClasses.add(partition % partitionSize);
      }
      assertEquals(partitionClasses.size(), 1,
          "Worker: " + workerId + " scans several partition classes: " + partitionClasses);
      int partitionClass = partitionClasses.iterator().next();
      Integer recorded = workerIdToClass.put(workerId, partitionClass);
      assertTrue(recorded == null || recorded == partitionClass, "Worker: " + workerId + " stands for partition class: "
          + recorded + " on one side of the exchange and: " + partitionClass + " on the other");
    }
  }

  private static Map<Integer, String> workerIdToServer(DispatchablePlanFragment leafFragment) {
    Map<Integer, String> workerIdToServer = new HashMap<>();
    for (Map.Entry<QueryServerInstance, List<Integer>> entry
        : leafFragment.getServerInstanceToWorkerIdMap().entrySet()) {
      for (Integer workerId : entry.getValue()) {
        workerIdToServer.put(workerId, entry.getKey().getInstanceId());
      }
    }
    return workerIdToServer;
  }

  private static List<String> assignedSegments(DispatchablePlanFragment leafFragment, int workerId) {
    List<String> segments = new ArrayList<>();
    leafFragment.getWorkerIdToSegmentsMap().get(workerId).values().forEach(segments::addAll);
    return segments;
  }

  /// Builds a QueryEnvironment for a hybrid partitioned table "testTable" (function Hashcode on col1, 4 partitions).
  /// Partition `p` holds offline segment `"segO{p}"` and realtime segment `"segR{p}"`, both fully
  /// replicated on server `p`. The given surviving segment lists are what the [RoutingManager] returns for
  /// the filtered routing query of each table type.
  private static QueryEnvironment newHybridPartitionedQueryEnvironment(List<String> survivingOfflineSegments,
      List<String> survivingRealtimeSegments) {
    return newHybridPartitionedQueryEnvironment(survivingOfflineSegments, survivingRealtimeSegments, List.of(),
        List.of());
  }

  /// Same as [#newHybridPartitionedQueryEnvironment(List, List)], with the segments reported as having invalid
  /// partition metadata for each table type.
  private static QueryEnvironment newHybridPartitionedQueryEnvironment(List<String> survivingOfflineSegments,
      List<String> survivingRealtimeSegments, List<String> offlineSegmentsWithInvalidPartition,
      List<String> realtimeSegmentsWithInvalidPartition) {
    return newHybridPartitionedQueryEnvironment(survivingOfflineSegments, survivingRealtimeSegments,
        offlineSegmentsWithInvalidPartition, realtimeSegmentsWithInvalidPartition, Set.of(), Set.of());
  }

  /// Same as [#newHybridPartitionedQueryEnvironment(List, List, List, List)], with the OFFLINE partitions that have no
  /// entry in the offline partition info map and, among those, the ones the broker reports as absent only because all
  /// of their segments are new and not fully online yet. The REALTIME side always has an entry for every partition.
  private static QueryEnvironment newHybridPartitionedQueryEnvironment(List<String> survivingOfflineSegments,
      List<String> survivingRealtimeSegments, List<String> offlineSegmentsWithInvalidPartition,
      List<String> realtimeSegmentsWithInvalidPartition, Set<Integer> emptyOfflinePartitions,
      Set<Integer> offlinePartitionsWithOnlyDeferredSegments) {
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
      if (!emptyOfflinePartitions.contains(p)) {
        offlinePartitions[p] = new TablePartitionReplicatedServersInfo.PartitionInfo(partitionServers,
            new ArrayList<>(List.of("segO" + p)));
      }
      realtimePartitions[p] = new TablePartitionReplicatedServersInfo.PartitionInfo(partitionServers,
          new ArrayList<>(List.of("segR" + p)));
    }
    String realtimeTableName = PARTITIONED_TABLE + "_REALTIME";
    TablePartitionReplicatedServersInfo offlineInfo = new TablePartitionReplicatedServersInfo(
        PARTITIONED_TABLE_OFFLINE, "col1", "Hashcode", numPartitions, offlinePartitions,
        offlineSegmentsWithInvalidPartition, offlinePartitionsWithOnlyDeferredSegments);
    TablePartitionReplicatedServersInfo realtimeInfo = new TablePartitionReplicatedServersInfo(
        realtimeTableName, "col1", "Hashcode", numPartitions, realtimePartitions,
        realtimeSegmentsWithInvalidPartition, Set.of());

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

  /// Buckets the given surviving segments onto their owning server (partition p's segment lives on server p).
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

  /// Builds a QueryEnvironment for an offline partitioned table "testTable" (function Hashcode on col1). Partition
  /// `p` holds one segment `"seg{p}"` fully replicated on the `replicasPerPartition` servers starting
  /// at `serverIdxPerPartition[p]`. `survivingSegments` is what the [RoutingManager] returns from
  /// getRoutingTable for the filtered routing query -- i.e. the segments that survive broker pruning; the corresponding
  /// partitions are kept. `unavailableSegments` are reported by the routing table as unavailable (they must
  /// keep their partition alive). When `throwOnRouting` is true, getRoutingTable throws to exercise the
  /// fail-open path.
  private static QueryEnvironment newPartitionedQueryEnvironment(int[] serverIdxPerPartition, int numServers,
      int replicasPerPartition, List<String> survivingSegments, List<String> unavailableSegments,
      int reportedPrunedByRouting, boolean throwOnRouting) {
    return newPartitionedQueryEnvironment(serverIdxPerPartition, numServers, replicasPerPartition, survivingSegments,
        unavailableSegments, reportedPrunedByRouting, throwOnRouting, Set.of(), Set.of());
  }

  /// Same as [#newPartitionedQueryEnvironment(int[], int, int, List, List, int, boolean)], with the partitions that
  /// have no entry in the partition info map at all (`emptyPartitions`) and, among those, the ones the broker reports
  /// as absent only because all of their segments are new and not fully online yet
  /// (`partitionsWithOnlyDeferredSegments`).
  private static QueryEnvironment newPartitionedQueryEnvironment(int[] serverIdxPerPartition, int numServers,
      int replicasPerPartition, List<String> survivingSegments, List<String> unavailableSegments,
      int reportedPrunedByRouting, boolean throwOnRouting, Set<Integer> emptyPartitions,
      Set<Integer> partitionsWithOnlyDeferredSegments) {
    return newPartitionedQueryEnvironment(serverIdxPerPartition, numServers, replicasPerPartition, survivingSegments,
        unavailableSegments, reportedPrunedByRouting, throwOnRouting, emptyPartitions,
        partitionsWithOnlyDeferredSegments, Map.of());
  }

  /// Same again, with the surviving segments a leaf sees when its own filter carries a given string literal. Lets two
  /// leaves scanning the SAME table be given different verdicts, which is the only way to tell a per-leaf pruning
  /// verdict from a per-table one.
  private static QueryEnvironment newPartitionedQueryEnvironment(int[] serverIdxPerPartition, int numServers,
      int replicasPerPartition, List<String> survivingSegments, List<String> unavailableSegments,
      int reportedPrunedByRouting, boolean throwOnRouting, Set<Integer> emptyPartitions,
      Set<Integer> partitionsWithOnlyDeferredSegments, Map<String, List<String>> survivingSegmentsByFilterLiteral) {
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
      if (emptyPartitions.contains(p)) {
        continue;
      }
      Set<String> fullyReplicatedServers = new HashSet<>();
      for (int r = 0; r < replicasPerPartition; r++) {
        fullyReplicatedServers.add(servers[serverIdxPerPartition[p] + r].getInstanceId());
      }
      // Mutable, like the lists the broker publishes: the assignment must hand out a copy rather than this instance.
      partitionInfoMap[p] = new TablePartitionReplicatedServersInfo.PartitionInfo(fullyReplicatedServers,
          new ArrayList<>(List.of("seg" + p)));
    }
    TablePartitionReplicatedServersInfo tablePartitionInfo = new TablePartitionReplicatedServersInfo(
        PARTITIONED_TABLE_OFFLINE, "col1", "Hashcode", numPartitions, partitionInfoMap, List.of(),
        partitionsWithOnlyDeferredSegments);

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
    survivingSegmentsByFilterLiteral.forEach(routingManager::survivingSegmentsForFilterLiteral);

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

  /// Returns the leaf table-scan fragment: the only fragment with segment assignments.
  @Nullable
  private static DispatchablePlanFragment leafFragment(DispatchableSubPlan dispatchableSubPlan) {
    List<DispatchablePlanFragment> leafFragments = leafFragments(dispatchableSubPlan);
    return leafFragments.isEmpty() ? null : leafFragments.get(0);
  }

  /// Returns all fragments with segment assignments (the table-scan leaves).
  private static List<DispatchablePlanFragment> leafFragments(DispatchableSubPlan dispatchableSubPlan) {
    List<DispatchablePlanFragment> leafFragments = new ArrayList<>();
    for (DispatchablePlanFragment fragment : dispatchableSubPlan.getQueryStageMap().values()) {
      if (!fragment.getWorkerIdToSegmentsMap().isEmpty()) {
        leafFragments.add(fragment);
      }
    }
    return leafFragments;
  }

  /// Maps each assigned segment to the instance id of the server its worker was placed on.
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

  /// Tests that literal-only stages (e.g. UNION ALL of constant values) are assigned to the same
  /// servers as the table-scanning stages, not to all enabled servers across all tenants.
  ///
  /// Simulates two server tenants: T1 (serves the queried table) and T2 (unrelated). Before the fix,
  /// literal-only stages processed before leaf stages would see an empty candidate set and fall back to
  /// all enabled servers, potentially landing on T2 servers.
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

  /// A RoutingManager that simulates two tenants of servers, where only one tenant serves the queried
  /// tables. `getEnabledServerInstanceMap()` returns all servers across both tenants, while
  /// `getServingInstances()` returns only the tenant's servers.
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

  /// A custom RoutingManager implementation that simulates a table with routing but no segments.
  /// This is used to test the empty leaf server fallback logic.
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

    /// When set, filter-bearing routing requests return an all-pruned (empty) routing table.
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

  /// A RoutingManager for the partitioned leaf path. It exposes a [TablePartitionReplicatedServersInfo] per typed
  /// table (driving `calculatePartitionTableInfo`) and returns a pre-configured [RoutingTable] of surviving
  /// segments from getRoutingTable (simulating what the real segment pruners would return for the query filter). This
  /// lets the test drive which partitions survive without depending on the pruner internals (which are tested
  /// separately).
  private static class PartitionedRoutingManager implements RoutingManager {
    private final Map<String, ServerInstance> _enabledServers;
    private final Map<String, TablePartitionReplicatedServersInfo> _partitionInfoByTable;
    private final Map<String, RoutingTable> _routingTableByTable;
    /// Surviving segments for a leaf whose filter carries the given string literal, which is how a test gives two
    /// leaves scanning ONE table two different verdicts. Falls back to the per-table routing table when absent.
    private final Map<String, List<String>> _survivingSegmentsByFilterLiteral = new HashMap<>();
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

    /// Only the replicated leaf path reads this (a table hinted `is_replicated` holds every segment on every worker),
    /// so answer it from the same partition layout the partitioned path reads.
    @Nullable
    @Override
    public List<String> getSegments(BrokerRequest brokerRequest) {
      return allSegments(brokerRequest.getQuerySource().getTableName());
    }

    /// Derives the pruned set the way `BaseBrokerRoutingManager` does -- everything selection offered minus what the
    /// pruners kept -- from the same configured routing table. Unavailable segments were selected and not pruned, so
    /// they stay out of it and keep their partition alive. A table with no routing table registered proves nothing,
    /// which is what an unexpected routing call should look like.
    @Override
    public Set<String> getPrunedSegments(BrokerRequest brokerRequest) {
      if (_throwOnRouting) {
        throw new RuntimeException("Simulated routing failure");
      }
      validatePrunableFilter(brokerRequest.getPinotQuery().getFilterExpression());
      String tableNameWithType = brokerRequest.getQuerySource().getTableName();
      Set<String> prunedSegments = new HashSet<>(allSegments(tableNameWithType));
      String filterLiteral = firstStringLiteral(brokerRequest.getPinotQuery().getFilterExpression());
      List<String> survivingSegments =
          filterLiteral != null ? _survivingSegmentsByFilterLiteral.get(filterLiteral) : null;
      if (survivingSegments != null) {
        prunedSegments.removeAll(survivingSegments);
        return prunedSegments;
      }
      RoutingTable routingTable = _routingTableByTable.get(tableNameWithType);
      if (routingTable == null) {
        return Set.of();
      }
      for (SegmentsToQuery segmentsToQuery : routingTable.getServerInstanceToSegmentsMap().values()) {
        prunedSegments.removeAll(segmentsToQuery.getSegments());
      }
      prunedSegments.removeAll(routingTable.getUnavailableSegments());
      return prunedSegments;
    }

    PartitionedRoutingManager survivingSegmentsForFilterLiteral(String filterLiteral, List<String> segments) {
      _survivingSegmentsByFilterLiteral.put(filterLiteral, segments);
      return this;
    }

    /// The first string literal in the filter, which the tests use as a stand-in for "which filter is this".
    @Nullable
    private static String firstStringLiteral(@Nullable Expression expression) {
      if (expression == null) {
        return null;
      }
      if (expression.getLiteral() != null && expression.getLiteral().isSetStringValue()) {
        return expression.getLiteral().getStringValue();
      }
      Function function = expression.getFunctionCall();
      if (function != null) {
        for (Expression operand : function.getOperands()) {
          String literal = firstStringLiteral(operand);
          if (literal != null) {
            return literal;
          }
        }
      }
      return null;
    }

    private List<String> allSegments(String tableNameWithType) {
      TablePartitionReplicatedServersInfo partitionInfo = _partitionInfoByTable.get(tableNameWithType);
      if (partitionInfo == null) {
        return List.of();
      }
      List<String> segments = new ArrayList<>();
      for (TablePartitionReplicatedServersInfo.PartitionInfo entry : partitionInfo.getPartitionInfoMap()) {
        if (entry != null) {
          segments.addAll(entry._segments);
        }
      }
      return segments;
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

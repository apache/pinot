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
package org.apache.pinot.tsdb.planner;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesDispatchablePlan;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesRoutingCustomizer;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfiguration;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanner;
import org.apache.pinot.tsdb.spi.TimeSeriesMetadata;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Plans a time-series query end to end with a routing customizer, and checks that what the customizer decides reaches
/// the plan the broker dispatches to servers.
@Test(singleThreaded = true)
public class TimeSeriesQueryEnvironmentTest {
  private static final String LANGUAGE = "test";
  private static final String TABLE = "myTable_OFFLINE";
  private static final TimeBuckets TIME_BUCKETS = TimeBuckets.ofSeconds(1000L, Duration.ofSeconds(10), 6);

  @AfterMethod
  public void tearDown() {
    // Planning initializes the TableScanVisitor singleton; leave it initialized for other tests.
    newEnvironment(mock(RoutingManager.class), mock(TableCache.class), TimeSeriesRoutingCustomizer.NO_OP);
  }

  @Test
  public void testDispatchedLeafCarriesRoutingDecisions() {
    ServerInstance server = mock(ServerInstance.class);
    when(server.getInstanceId()).thenReturn("Server_1");
    RoutingManager routingManager = mock(RoutingManager.class);
    when(routingManager.routingExists(TABLE)).thenReturn(true);
    Map<ServerInstance, SegmentsToQuery> routed = new HashMap<>();
    routed.put(server, new SegmentsToQuery(List.of("seg1", "seg2"), List.of()));
    when(routingManager.getRoutingTableWithMissingServerSegments(any(BrokerRequest.class), anyLong()))
        .thenReturn(new RoutingTable(routed, List.of(), 0));
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableConfig(TABLE))
        .thenReturn(new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build());
    // Keeps only seg1, and pins the version the request asked for.
    TimeSeriesRoutingCustomizer customizer = (leaf, routingTable, context) -> {
      context.putRoutingQueryOption("pinnedVersion", context.getRequestQueryOptions().get("pinnedVersion"));
      return new RoutingTable(Map.of(server, new SegmentsToQuery(List.of("seg1"), List.of())), List.of(), 1);
    };
    TimeSeriesQueryEnvironment environment = newEnvironment(routingManager, tableCache, customizer);
    RangeTimeSeriesRequest request = new RangeTimeSeriesRequest(LANGUAGE, "q", 1000L, 1060L, 10L,
        Duration.ofSeconds(10), 100, 100, "q", Map.of("pinnedVersion", "7"));
    RequestContext requestContext = mock(RequestContext.class);
    when(requestContext.getRequestId()).thenReturn(1L);

    TimeSeriesDispatchablePlan plan = environment.buildPhysicalPlan(request, requestContext,
        new TimeSeriesLogicalPlanResult(leaf("myTable"), TIME_BUCKETS), tableNames -> { });

    assertEquals(plan.getLeafIdToSegmentsByInstanceId(), Map.of("Server_1", Map.of("leaf", List.of("seg1"))));
    assertEquals(plan.getServerFragments().size(), 1);
    LeafTimeSeriesPlanNode dispatchedLeaf = (LeafTimeSeriesPlanNode) plan.getServerFragments().get(0);
    assertEquals(dispatchedLeaf.getTableName(), TABLE);
    assertEquals(dispatchedLeaf.getQueryOptions(), Map.of("numGroupsLimit", "100", "pinnedVersion", "7"));
  }

  @Test
  public void testLeafNarrowedToNothingYieldsNoServersInsteadOfFailing() {
    RoutingManager routingManager = routingManagerWith(Map.of(mockServer("Server_1"), List.of("seg1")));
    TimeSeriesQueryEnvironment environment = newEnvironment(routingManager, tableCache(),
        (leaf, routingTable, context) -> new RoutingTable(Map.of(), List.of(), 1));

    TimeSeriesDispatchablePlan plan = environment.buildPhysicalPlan(request(), requestContext(),
        new TimeSeriesLogicalPlanResult(leaf("leaf", "myTable"), TIME_BUCKETS), tableNames -> { });

    // Nothing is dispatched; the broker's exchange for the leaf expects no server, so the query has no data.
    assertTrue(plan.getQueryServerInstances().isEmpty());
    assertEquals(plan.getNumInputServersForExchangePlanNode(), Map.of("leaf", 0));
  }

  @Test
  public void testOnlyTheLeafWithoutServersExpectsNone() {
    ServerInstance serverOne = mockServer("Server_1");
    ServerInstance serverTwo = mockServer("Server_2");
    RoutingManager routingManager =
        routingManagerWith(Map.of(serverOne, List.of("seg1"), serverTwo, List.of("seg2")));
    // The right leaf is narrowed to nothing; the left one keeps both servers.
    TimeSeriesQueryEnvironment environment = newEnvironment(routingManager, tableCache(),
        (leaf, routingTable, context) -> leaf.getId().equals("right") ? new RoutingTable(Map.of(), List.of(), 2)
            : routingTable);
    BaseTimeSeriesPlanNode root =
        new TestPlanNode("root", List.of(leaf("left", "myTable"), leaf("right", "myTable")));

    TimeSeriesDispatchablePlan plan = environment.buildPhysicalPlan(request(), requestContext(),
        new TimeSeriesLogicalPlanResult(root, TIME_BUCKETS), tableNames -> { });

    assertEquals(plan.getQueryServerInstances().size(), 2);
    assertEquals(plan.getNumInputServersForExchangePlanNode(), Map.of("left", 2, "right", 0));
  }

  @Test
  public void testServerRunsOnlyTheLeavesItHoldsSegmentsFor() {
    ServerInstance serverOne = mockServer("Server_1");
    ServerInstance serverTwo = mockServer("Server_2");
    RoutingManager routingManager =
        routingManagerWith(Map.of(serverOne, List.of("seg1"), serverTwo, List.of("seg2")));
    // Each leaf keeps one server: left reads Server_1, right reads Server_2.
    TimeSeriesQueryEnvironment environment = newEnvironment(routingManager, tableCache(),
        (leaf, routingTable, context) -> new RoutingTable(
            Map.of(leaf.getId().equals("left") ? serverOne : serverTwo,
                new SegmentsToQuery(List.of(leaf.getId().equals("left") ? "seg1" : "seg2"), List.of())),
            List.of(), 1));
    BaseTimeSeriesPlanNode root =
        new TestPlanNode("root", List.of(leaf("left", "myTable"), leaf("right", "myTable")));

    TimeSeriesDispatchablePlan plan = environment.buildPhysicalPlan(request(), requestContext(),
        new TimeSeriesLogicalPlanResult(root, TIME_BUCKETS), tableNames -> { });

    // Each exchange waits for one block, so each server must run, and send a block for, only its own leaf.
    assertEquals(plan.getNumInputServersForExchangePlanNode(), Map.of("left", 1, "right", 1));
    assertEquals(fragmentIds(plan.getSerializedServerFragments("Server_1")), List.of("left"));
    assertEquals(fragmentIds(plan.getSerializedServerFragments("Server_2")), List.of("right"));
  }

  @Test
  public void testOneServerRunsTheWholePlanOnlyWhenItHoldsEveryLeaf() {
    RoutingManager routingManager = routingManagerWith(Map.of(mockServer("Server_1"), List.of("seg1")));
    BaseTimeSeriesPlanNode root =
        new TestPlanNode("root", List.of(leaf("left", "myTable"), leaf("right", "myTable")));

    // Both leaves route to Server_1, so it runs the whole plan.
    TimeSeriesDispatchablePlan wholePlan =
        newEnvironment(routingManager, tableCache(), TimeSeriesRoutingCustomizer.NO_OP).buildPhysicalPlan(
            request(), requestContext(), new TimeSeriesLogicalPlanResult(root, TIME_BUCKETS), tableNames -> { });
    assertEquals(fragmentIds(wholePlan.getSerializedServerFragments("Server_1")), List.of("root"));

    // The right leaf is narrowed to nothing: Server_1 runs only the left leaf, and the right leaf reads nothing
    // rather than whatever Server_1 holds of its table.
    TimeSeriesQueryEnvironment environment = newEnvironment(routingManager, tableCache(),
        (leaf, routingTable, context) -> leaf.getId().equals("right") ? new RoutingTable(Map.of(), List.of(), 1)
            : routingTable);
    TimeSeriesDispatchablePlan plan = environment.buildPhysicalPlan(request(), requestContext(),
        new TimeSeriesLogicalPlanResult(root, TIME_BUCKETS), tableNames -> { });
    assertEquals(fragmentIds(plan.getSerializedServerFragments("Server_1")), List.of("left"));
    assertEquals(plan.getNumInputServersForExchangePlanNode(), Map.of("left", 1, "right", 0));
  }

  @Test
  public void testCustomizerCannotAddAnOptionalOnlyRpc() {
    ServerInstance serverOne = mockServer("Server_1");
    ServerInstance serverTwo = mockServer("Server_2");
    RoutingManager routingManager = routingManagerWith(Map.of(serverOne, List.of("seg1"), serverTwo, List.of("seg2")));
    TimeSeriesQueryEnvironment environment = newEnvironment(routingManager, tableCache(),
        (leaf, routingTable, context) -> new RoutingTable(Map.of(
            serverOne, new SegmentsToQuery(List.of("seg1"), List.of("optional1")),
            serverTwo, new SegmentsToQuery(List.of(), List.of("optional2"))), List.of(), 1));

    TimeSeriesDispatchablePlan plan = environment.buildPhysicalPlan(request(), requestContext(),
        new TimeSeriesLogicalPlanResult(leaf("leaf", "myTable"), TIME_BUCKETS), tableNames -> { });

    assertEquals(plan.getNumInputServersForExchangePlanNode(), Map.of("leaf", 1));
    assertEquals(plan.getQueryServerInstances().size(), 1);
    assertEquals(plan.getQueryServerInstances().get(0).getInstanceId(), "Server_1");
    assertEquals(plan.getLeafIdToOptionalSegmentsByInstanceId(),
        Map.of("Server_1", Map.of("leaf", List.of("optional1"))));
  }

  @Test
  public void testTablesAreAuthorizedBeforeRouting() {
    List<String> events = new ArrayList<>();
    RoutingManager routingManager = routingManagerWith(Map.of(mockServer("Server_1"), List.of("seg1")));
    TimeSeriesQueryEnvironment environment = newEnvironment(routingManager, tableCache(),
        (leaf, routingTable, context) -> {
          events.add("customize " + leaf.getTableName());
          return routingTable;
        });

    environment.buildPhysicalPlan(request(), requestContext(),
        new TimeSeriesLogicalPlanResult(leaf("leaf", "myTable"), TIME_BUCKETS),
        tableNames -> events.add("authorize " + tableNames));
    assertEquals(events, List.of("authorize [" + TABLE + "]", "customize " + TABLE));

    // A caller who may not query the table never reaches the customizer.
    events.clear();
    assertThrows(SecurityException.class, () -> environment.buildPhysicalPlan(request(), requestContext(),
        new TimeSeriesLogicalPlanResult(leaf("leaf", "myTable"), TIME_BUCKETS), tableNames -> {
          throw new SecurityException("denied");
        }));
    assertEquals(events, List.of());
  }

  @Test
  public void testLeafWithOnlyUnavailableSegmentsFailsTheQuery() {
    // No server can serve the leaf's segments: an outage, which must not read as a range without data.
    RoutingManager routingManager = mock(RoutingManager.class);
    when(routingManager.routingExists(TABLE)).thenReturn(true);
    when(routingManager.getRoutingTableWithMissingServerSegments(any(BrokerRequest.class), anyLong()))
        .thenReturn(new RoutingTable(Map.of(), List.of("seg1", "seg2"), 0));
    TimeSeriesQueryEnvironment environment =
        newEnvironment(routingManager, tableCache(), TimeSeriesRoutingCustomizer.NO_OP);

    QueryException e = expectThrows(QueryException.class, () -> environment.buildPhysicalPlan(request(),
        requestContext(), new TimeSeriesLogicalPlanResult(leaf("leaf", "myTable"), TIME_BUCKETS), tableNames -> { }));
    assertEquals(e.getErrorCode(), QueryErrorCode.SERVER_SEGMENT_MISSING);
  }

  @DataProvider
  public Object[][] optionalSegments() {
    return new Object[][]{{List.of()}, {List.of("optional")}};
  }

  @Test(dataProvider = "optionalSegments")
  public void testServerEntriesWithoutRequiredSegmentsDoNotHideUnavailableSegments(List<String> optionalSegments) {
    ServerInstance server = mockServer("Server_1");
    RoutingManager routingManager = routingManagerWith(Map.of(server, List.of("seg1")));
    // The customizer keeps the server but clears its required segments, and seg2 has no server.
    TimeSeriesQueryEnvironment environment = newEnvironment(routingManager, tableCache(),
        (leaf, routingTable, context) -> new RoutingTable(
            Map.of(server, new SegmentsToQuery(List.of(), optionalSegments)), List.of("seg2"), 1));

    QueryException e = expectThrows(QueryException.class, () -> environment.buildPhysicalPlan(request(),
        requestContext(), new TimeSeriesLogicalPlanResult(leaf("leaf", "myTable"), TIME_BUCKETS), tableNames -> { }));
    assertEquals(e.getErrorCode(), QueryErrorCode.SERVER_SEGMENT_MISSING);
  }

  /// The ids of the serialized fragments' root nodes; reads the JSON, since a [TestPlanNode] does not deserialize.
  private static List<String> fragmentIds(List<String> serializedFragments) {
    return serializedFragments.stream().map(fragment -> {
      try {
        return JsonUtils.stringToJsonNode(fragment).get("id").asText();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }).toList();
  }

  private static RoutingManager routingManagerWith(Map<ServerInstance, List<String>> segmentsByServer) {
    RoutingManager routingManager = mock(RoutingManager.class);
    when(routingManager.routingExists(TABLE)).thenReturn(true);
    Map<ServerInstance, SegmentsToQuery> routed = new HashMap<>();
    segmentsByServer.forEach((server, segments) -> routed.put(server, new SegmentsToQuery(segments, List.of())));
    when(routingManager.getRoutingTableWithMissingServerSegments(any(BrokerRequest.class), anyLong()))
        .thenReturn(new RoutingTable(routed, List.of(), 0));
    return routingManager;
  }

  private static ServerInstance mockServer(String instanceId) {
    ServerInstance server = mock(ServerInstance.class);
    when(server.getInstanceId()).thenReturn(instanceId);
    return server;
  }

  private static TableCache tableCache() {
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableConfig(TABLE))
        .thenReturn(new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build());
    return tableCache;
  }

  private static RangeTimeSeriesRequest request() {
    return new RangeTimeSeriesRequest(LANGUAGE, "q", 1000L, 1060L, 10L, Duration.ofSeconds(10), 100, 100, "q",
        Map.of());
  }

  private static RequestContext requestContext() {
    RequestContext requestContext = mock(RequestContext.class);
    when(requestContext.getRequestId()).thenReturn(1L);
    return requestContext;
  }

  private static TimeSeriesQueryEnvironment newEnvironment(RoutingManager routingManager, TableCache tableCache,
      TimeSeriesRoutingCustomizer customizer) {
    PinotConfiguration config = new PinotConfiguration(Map.of(
        PinotTimeSeriesConfiguration.getEnabledLanguagesConfigKey(), LANGUAGE,
        PinotTimeSeriesConfiguration.getLogicalPlannerConfigKey(LANGUAGE), NoOpPlanner.class.getName()));
    TimeSeriesQueryEnvironment environment =
        new TimeSeriesQueryEnvironment(config, routingManager, tableCache, customizer);
    environment.init(config);
    return environment;
  }

  private static LeafTimeSeriesPlanNode leaf(String tableName) {
    return leaf("leaf", tableName);
  }

  private static LeafTimeSeriesPlanNode leaf(String id, String tableName) {
    return new LeafTimeSeriesPlanNode(id, List.of(), tableName, "ts", TimeUnit.SECONDS, 0L, "", "value",
        new AggInfo("SUM", false, Map.of()), List.of(), 100, Map.of("numGroupsLimit", "100"));
  }

  /// The planner the environment requires for a configured language; this test builds its logical plan directly.
  public static class NoOpPlanner implements TimeSeriesLogicalPlanner {
    @Override
    public void init(PinotConfiguration pinotConfiguration) {
    }

    @Override
    public TimeSeriesLogicalPlanResult plan(RangeTimeSeriesRequest request, TimeSeriesMetadata metadata) {
      throw new UnsupportedOperationException();
    }
  }

  private static class TestPlanNode extends BaseTimeSeriesPlanNode {
    TestPlanNode(String id, List<BaseTimeSeriesPlanNode> inputs) {
      super(id, inputs);
    }

    @Override
    public BaseTimeSeriesPlanNode withInputs(List<BaseTimeSeriesPlanNode> newInputs) {
      return new TestPlanNode(_id, newInputs);
    }

    @Override
    public String getKlass() {
      return "";
    }

    @Override
    public String getExplainName() {
      return "";
    }

    @Override
    public BaseTimeSeriesOperator run() {
      return null;
    }
  }
}

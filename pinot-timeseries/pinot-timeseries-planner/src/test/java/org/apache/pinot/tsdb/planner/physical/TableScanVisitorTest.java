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
package org.apache.pinot.tsdb.planner.physical;

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
import org.apache.pinot.core.routing.TableRouteProvider;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Uses the [TableScanVisitor] singleton, so the methods must not run in parallel.
@Test(singleThreaded = true)
public class TableScanVisitorTest {
  private static final String TABLE = "myTable_OFFLINE";
  private static final TimeBuckets TIME_BUCKETS = TimeBuckets.ofSeconds(1000L, Duration.ofSeconds(10), 6);

  private RoutingManager _routingManager;
  private TableCache _tableCache;
  private ServerInstance _serverOne;
  private ServerInstance _serverTwo;

  @BeforeMethod
  public void setUp() {
    _routingManager = mock(RoutingManager.class);
    _tableCache = mock(TableCache.class);
    when(_tableCache.getTableConfig(TABLE)).thenReturn(mock(TableConfig.class));
    _serverOne = mock(ServerInstance.class);
    _serverTwo = mock(ServerInstance.class);
    when(_serverOne.getInstanceId()).thenReturn("Server_1");
    when(_serverTwo.getInstanceId()).thenReturn("Server_2");
    Map<ServerInstance, SegmentsToQuery> routed = new HashMap<>();
    routed.put(_serverOne, new SegmentsToQuery(new ArrayList<>(List.of("seg1", "seg2")), List.of()));
    routed.put(_serverTwo, new SegmentsToQuery(List.of("seg3"), List.of()));
    when(_routingManager.getRoutingTableWithMissingServerSegments(any(BrokerRequest.class), anyLong()))
        .thenReturn(new RoutingTable(routed, List.of(), 0));
  }

  @AfterMethod
  public void tearDown() {
    // The visitor is a singleton; leave it initialized for other tests.
    TableScanVisitor.INSTANCE.init(_routingManager, mock(TableRouteProvider.class), _tableCache);
  }

  @Test
  public void testWithoutCustomizerRoutesAsChosenAndKeepsPlan() {
    TableScanVisitor.INSTANCE.init(_routingManager, mock(TableRouteProvider.class), _tableCache);
    LeafTimeSeriesPlanNode leaf = leaf("leaf", Map.of("k", "v"));
    TableScanVisitor.Context context =
        TableScanVisitor.createContext(1L, Map.of("requestOption", "x"), TimeSeriesRoutingCustomizer.NO_OP);

    TableScanVisitor.INSTANCE.assignSegmentsToPlan(leaf, TIME_BUCKETS, context);

    assertEquals(context.getLeafIdToSegmentsByInstanceId(),
        Map.of("Server_1", Map.of("leaf", List.of("seg1", "seg2")), "Server_2", Map.of("leaf", List.of("seg3"))));
    assertTrue(context.getRoutingQueryOptions().isEmpty());
    assertSame(TableScanVisitor.INSTANCE.addQueryOptionsToPlan(leaf, context.getRoutingQueryOptions()), leaf);
  }

  @Test
  public void testCustomizerNarrowsSegmentsAndSendsOptionsToEveryLeaf() {
    List<String> calls = new ArrayList<>();
    TimeSeriesRoutingCustomizer customizer = (leaf, routingTable, context) -> {
      // The customizer sees the leaf, the request's options and what earlier leaves added.
      calls.add(leaf.getId() + ":" + leaf.getTableName() + ":" + context.getRoutingQueryOptions().get("pinnedVersion"));
      if (!context.getRoutingQueryOptions().containsKey("pinnedVersion")) {
        context.putRoutingQueryOption("pinnedVersion",
            context.getRequestQueryOptions().getOrDefault("pinnedVersion", "42"));
      }
      context.putRoutingQueryOption("k", "fromCustomizer");
      Map<ServerInstance, SegmentsToQuery> narrowed = new HashMap<>();
      routingTable.getServerInstanceToSegmentsMap().forEach((server, segments) -> narrowed.put(server,
          new SegmentsToQuery(segments.getSegments().stream().filter(s -> !s.equals("seg2")).toList(), List.of())));
      return new RoutingTable(narrowed, List.of(), 1);
    };
    TableScanVisitor.INSTANCE.init(_routingManager, mock(TableRouteProvider.class), _tableCache);
    LeafTimeSeriesPlanNode leafOne = leaf("leafOne", Map.of("k", "v", "own", "kept"));
    LeafTimeSeriesPlanNode leafTwo = leaf("leafTwo", null);
    BaseTimeSeriesPlanNode root = new TestPlanNode("root", List.of(leafOne, leafTwo));
    TableScanVisitor.Context context = TableScanVisitor.createContext(1L, Map.of("pinnedVersion", "7"), customizer);

    TableScanVisitor.INSTANCE.assignSegmentsToPlan(root, TIME_BUCKETS, context);
    BaseTimeSeriesPlanNode planWithOptions =
        TableScanVisitor.INSTANCE.addQueryOptionsToPlan(root, context.getRoutingQueryOptions());

    // Both leaves read the narrowed segments; the first leaf honored the request's pin and the second saw it.
    assertEquals(context.getLeafIdToSegmentsByInstanceId(), Map.of(
        "Server_1", Map.of("leafOne", List.of("seg1"), "leafTwo", List.of("seg1")),
        "Server_2", Map.of("leafOne", List.of("seg3"), "leafTwo", List.of("seg3"))));
    assertEquals(calls, List.of("leafOne:" + TABLE + ":null", "leafTwo:" + TABLE + ":7"));
    // Every leaf sends the routing options, which replace a leaf's own option with the same key; the leaf keeps its
    // other options.
    List<BaseTimeSeriesPlanNode> leaves = planWithOptions.getInputs();
    assertEquals(leaves.get(0).getId(), "leafOne");
    assertEquals(((LeafTimeSeriesPlanNode) leaves.get(0)).getQueryOptions(),
        Map.of("pinnedVersion", "7", "k", "fromCustomizer", "own", "kept"));
    assertEquals(leaves.get(1).getId(), "leafTwo");
    assertEquals(((LeafTimeSeriesPlanNode) leaves.get(1)).getQueryOptions(),
        Map.of("pinnedVersion", "7", "k", "fromCustomizer"));
  }

  @Test
  public void testCustomizerCannotReachPlannerState() {
    TableScanVisitor.Context context =
        TableScanVisitor.createContext(1L, Map.of("k", "v"), TimeSeriesRoutingCustomizer.NO_OP);
    TimeSeriesRoutingContext view = context.getRoutingContext();

    view.putRoutingQueryOption("added", "x");

    assertEquals(context.getRoutingQueryOptions(), Map.of("added", "x"));
    assertThrows(UnsupportedOperationException.class, () -> view.getRoutingQueryOptions().clear());
    assertTrue(!(view instanceof TableScanVisitor.Context), "the view must not expose the planner's context");
  }

  @Test
  public void testServersRequireARequiredSegment() {
    ServerInstance serverThree = mock(ServerInstance.class);
    when(serverThree.getInstanceId()).thenReturn("Server_3");
    Map<ServerInstance, SegmentsToQuery> routed = new HashMap<>();
    routed.put(_serverOne, new SegmentsToQuery(List.of("seg1"), List.of("optional1")));
    // Newly pushed segments, not yet ONLINE in the external view: only optional.
    routed.put(_serverTwo, new SegmentsToQuery(List.of(), List.of("seg2")));
    routed.put(serverThree, new SegmentsToQuery(List.of(), List.of()));
    when(_routingManager.getRoutingTableWithMissingServerSegments(any(BrokerRequest.class), anyLong()))
        .thenReturn(new RoutingTable(routed, List.of(), 0));
    TableScanVisitor.INSTANCE.init(_routingManager, mock(TableRouteProvider.class), _tableCache);
    TableScanVisitor.Context context = TableScanVisitor.createContext(1L, Map.of(), TimeSeriesRoutingCustomizer.NO_OP);

    TableScanVisitor.INSTANCE.assignSegmentsToPlan(leaf("leaf", Map.of()), TIME_BUCKETS, context);

    // Optional-only and empty entries must not require an RPC.
    assertEquals(context.getLeafIdToSegmentsByInstanceId(), Map.of("Server_1", Map.of("leaf", List.of("seg1"))));
    assertEquals(context.getLeafIdToOptionalSegmentsByInstanceId(),
        Map.of("Server_1", Map.of("leaf", List.of("optional1"))));
    assertEquals(context.getQueryServers().size(), 1);
  }

  @Test
  public void testUpsertCustomizerCanAddOptionsButCannotNarrowAssignments() {
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.isUpsertEnabled()).thenReturn(true);
    when(_tableCache.getTableConfig(TABLE)).thenReturn(tableConfig);
    TableScanVisitor.INSTANCE.init(_routingManager, mock(TableRouteProvider.class), _tableCache);
    LeafTimeSeriesPlanNode leaf = leaf("leaf", Map.of());
    TableScanVisitor.Context optionsOnly = TableScanVisitor.createContext(1L, Map.of(),
        (leafNode, routingTable, routingContext) -> {
          routingContext.putRoutingQueryOption("pinnedVersion", "7");
          return routingTable;
        });
    TableScanVisitor.INSTANCE.assignSegmentsToPlan(leaf, TIME_BUCKETS, optionsOnly);
    assertEquals(optionsOnly.getRoutingQueryOptions(), Map.of("pinnedVersion", "7"));

    TableScanVisitor.Context narrowed = TableScanVisitor.createContext(1L, Map.of(),
        (leafNode, routingTable, routingContext) -> {
          routingTable.getServerInstanceToSegmentsMap().get(_serverOne).getSegments().remove("seg2");
          return routingTable;
        });
    QueryException failure = expectThrows(QueryException.class,
        () -> TableScanVisitor.INSTANCE.assignSegmentsToPlan(leaf, TIME_BUCKETS, narrowed));
    assertEquals(failure.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
  }

  @Test
  public void testLaterCustomizerCannotChangeEarlierUpsertLeafAssignments() {
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.isUpsertEnabled()).thenReturn(true);
    when(_tableCache.getTableConfig(TABLE)).thenReturn(tableConfig);
    List<String> earlierRequired = new ArrayList<>(List.of("seg1"));
    List<String> earlierOptional = new ArrayList<>(List.of("optional1"));
    when(_routingManager.getRoutingTableWithMissingServerSegments(any(BrokerRequest.class), anyLong())).thenReturn(
        new RoutingTable(Map.of(_serverOne, new SegmentsToQuery(earlierRequired, earlierOptional)), List.of(), 0),
        new RoutingTable(Map.of(_serverOne, new SegmentsToQuery(List.of("seg2"), List.of())), List.of(), 0));
    TableScanVisitor.INSTANCE.init(_routingManager, mock(TableRouteProvider.class), _tableCache);
    TableScanVisitor.Context context = TableScanVisitor.createContext(1L, Map.of(),
        (leafNode, routingTable, routingContext) -> {
          if (leafNode.getId().equals("later")) {
            earlierRequired.clear();
            earlierOptional.clear();
          }
          return routingTable;
        });

    BaseTimeSeriesPlanNode root = new TestPlanNode("root", List.of(leaf("earlier", Map.of()), leaf("later", Map.of())));
    TableScanVisitor.INSTANCE.assignSegmentsToPlan(root, TIME_BUCKETS, context);

    assertEquals(context.getLeafIdToSegmentsByInstanceId(),
        Map.of("Server_1", Map.of("earlier", List.of("seg1"), "later", List.of("seg2"))));
    assertEquals(context.getLeafIdToOptionalSegmentsByInstanceId(),
        Map.of("Server_1", Map.of("earlier", List.of("optional1"))));
  }

  @Test
  public void testRequestQueryOptionsAreReadOnly() {
    Map<String, String> requestQueryOptions = new HashMap<>(Map.of("k", "v"));
    TableScanVisitor.Context context =
        TableScanVisitor.createContext(1L, requestQueryOptions, TimeSeriesRoutingCustomizer.NO_OP);
    requestQueryOptions.put("k", "changedAfterwards");

    assertEquals(context.getRequestQueryOptions(), Map.of("k", "v"));
    assertThrows(UnsupportedOperationException.class, () -> context.getRequestQueryOptions().put("k", "x"));
    assertEquals(TableScanVisitor.createContext(1L, null, TimeSeriesRoutingCustomizer.NO_OP).getRequestQueryOptions(),
        Map.of());
  }

  private static LeafTimeSeriesPlanNode leaf(String id, Map<String, String> queryOptions) {
    return new LeafTimeSeriesPlanNode(id, List.of(), TABLE, "ts", TimeUnit.SECONDS, 0L, "", "value", null, List.of(),
        100, queryOptions);
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

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
package org.apache.pinot.broker.requesthandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Unit coverage for broker startup warmup: config parsing (including the concurrency and minIterations
/// knobs and their floors), the no-op interface default, the greedy table selection that keeps warmup
/// bounded on a large tenant, and the server-coverage check that flags when `maxTables` leaves servers
/// unprobed.
public class BrokerWarmupTest {

  @Test
  public void warmupIsDisabledByDefault() {
    BrokerWarmupConfig config = BrokerWarmupConfig.from(new PinotConfiguration());
    assertFalse(config.enabled());
    // Defaults still parse, so flipping the flag alone yields sane behavior.
    assertEquals(config.budgetMs(), Broker.DEFAULT_BROKER_STARTUP_WARMUP_BUDGET_MS);
    assertEquals(config.minIterations(), Broker.DEFAULT_BROKER_STARTUP_WARMUP_MIN_ITERATIONS);
    assertEquals(config.maxTables(), Broker.DEFAULT_BROKER_STARTUP_WARMUP_MAX_TABLES);
    // Concurrency defaults to serial.
    assertEquals(config.concurrency(), Broker.DEFAULT_BROKER_STARTUP_WARMUP_CONCURRENCY);
    assertEquals(config.concurrency(), 1);
    assertFalse(config.hasCustomQuery());
    assertFalse(config.hasCustomTables());
  }

  @Test
  public void configIsReadFromProperties() {
    PinotConfiguration properties = new PinotConfiguration();
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_ENABLED, true);
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_BUDGET_MS, 4321L);
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_MIN_ITERATIONS, 250);
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_MAX_TABLES, 3);
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_CONCURRENCY, 12);

    BrokerWarmupConfig config = BrokerWarmupConfig.from(properties);
    assertTrue(config.enabled());
    assertEquals(config.budgetMs(), 4321L);
    assertEquals(config.minIterations(), 250);
    assertEquals(config.maxTables(), 3);
    assertEquals(config.concurrency(), 12);
  }

  /// concurrency, minIterations and maxTables are floored at 1 even if misconfigured to 0/negative, so the
  /// probe pool, the exit floor, and the set-cover cap are always valid (maxTables=0 would otherwise make
  /// auto-select probe nothing and waste the whole budget backing off).
  @Test
  public void concurrencyMinIterationsAndMaxTablesFlooredAtOne() {
    PinotConfiguration properties = new PinotConfiguration();
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_CONCURRENCY, 0);
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_MIN_ITERATIONS, -5);
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_MAX_TABLES, 0);
    BrokerWarmupConfig config = BrokerWarmupConfig.from(properties);
    assertEquals(config.concurrency(), 1);
    assertEquals(config.minIterations(), 1);
    assertEquals(config.maxTables(), 1);
  }

  /// concurrency is clamped to an upper bound so a fat-fingered value cannot ask for a pathological thread
  /// pool (an OutOfMemoryError creating native threads) at startup.
  @Test
  public void concurrencyIsClampedToAnUpperBound() {
    PinotConfiguration properties = new PinotConfiguration();
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_CONCURRENCY, 1_000_000);
    BrokerWarmupConfig config = BrokerWarmupConfig.from(properties);
    assertTrue(config.concurrency() >= 1 && config.concurrency() <= 64,
        "concurrency must be clamped into [1, 64] but was " + config.concurrency());
  }

  /// Empty query/tables mean auto-select; a set query wins and a table list parses (trimmed, no blanks).
  @Test
  public void queryAndTablesOverridesParse() {
    PinotConfiguration properties = new PinotConfiguration();
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_QUERY, "SELECT count(*) FROM t");
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_TABLES, " a_OFFLINE , b_REALTIME ,");
    BrokerWarmupConfig config = BrokerWarmupConfig.from(properties);
    assertTrue(config.hasCustomQuery());
    assertEquals(config.queries(), List.of("SELECT count(*) FROM t"));
    assertTrue(config.hasCustomTables());
    assertEquals(config.tables(), List.of("a_OFFLINE", "b_REALTIME"));
  }

  /// Multiple probe queries separate on ';' (trimmed, no blanks, trailing ';' ignored); a comma inside a
  /// query does not split it, since ';' is the only separator (comma is the config layer's list delimiter).
  @Test
  public void multipleQueriesParseOnSemicolon() {
    PinotConfiguration properties = new PinotConfiguration();
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_QUERY,
        "SELECT count(*) FROM t1 ; SELECT max(x) FROM t2 ;");
    BrokerWarmupConfig config = BrokerWarmupConfig.from(properties);
    assertTrue(config.hasCustomQuery());
    assertEquals(config.queries(), List.of("SELECT count(*) FROM t1", "SELECT max(x) FROM t2"));

    // A single query containing a comma stays one query.
    PinotConfiguration single = new PinotConfiguration();
    single.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_QUERY, "SELECT a, b FROM t");
    assertEquals(BrokerWarmupConfig.from(single).queries().size(), 1);
  }

  /// A handler that does not override warmUp must report warm immediately. If the default blocked or
  /// returned false, every non-single-stage handler would hold readiness shut forever.
  @Test
  public void defaultWarmUpIsANoOp() {
    BrokerRequestHandler handler = mock(BrokerRequestHandler.class);
    when(handler.warmUp(any(), anyLong())).thenCallRealMethod();
    assertTrue(handler.warmUp(new BrokerWarmupConfig(true, 1L, 1, 1, List.of(), List.of(), 1),
        System.currentTimeMillis() + 1_000L));
  }

  /// Three tables, three servers, one table per server: all three must be picked, since dropping any one
  /// leaves a broker-to-server channel unwarmed.
  @Test
  public void selectionCoversEveryServer() {
    RoutingManager routing = routing(
        Map.of("a_OFFLINE", Set.of("s1"), "b_OFFLINE", Set.of("s2"), "c_OFFLINE", Set.of("s3")),
        Set.of("s1", "s2", "s3"));

    assertEquals(new java.util.HashSet<>(SingleConnectionBrokerRequestHandler.selectProbeTables(routing, 5)),
        Set.of("a_OFFLINE", "b_OFFLINE", "c_OFFLINE"));
  }

  /// A table adding no new server is skipped: probing it costs a query and warms nothing extra.
  @Test
  public void selectionSkipsRedundantTables() {
    RoutingManager routing = routing(
        Map.of("a_OFFLINE", Set.of("s1", "s2"), "b_OFFLINE", Set.of("s1"), "c_OFFLINE", Set.of("s2")),
        Set.of("s1", "s2"));

    // "a_OFFLINE" sorts first and already covers both servers, so nothing else is needed.
    assertEquals(SingleConnectionBrokerRequestHandler.selectProbeTables(routing, 5), List.of("a_OFFLINE"));
  }

  /// The cap keeps a tenant with thousands of tables from turning startup into a query storm.
  @Test
  public void selectionRespectsMaxTables() {
    RoutingManager routing = routing(
        Map.of("a_OFFLINE", Set.of("s1"), "b_OFFLINE", Set.of("s2"), "c_OFFLINE", Set.of("s3"),
            "d_OFFLINE", Set.of("s4")),
        Set.of("s1", "s2", "s3", "s4"));

    assertEquals(SingleConnectionBrokerRequestHandler.selectProbeTables(routing, 2).size(), 2);
    assertTrue(SingleConnectionBrokerRequestHandler.selectProbeTables(routing, 0).isEmpty());
  }

  /// Warmup runs before the first query and may legitimately find nothing routable yet.
  @Test
  public void selectionHandlesEmptyCluster() {
    assertTrue(SingleConnectionBrokerRequestHandler.selectProbeTables(routing(Map.of(), Set.of()), 5).isEmpty());
  }

  /// Must not blow up on a table whose serving-instance set is empty or unknown.
  @Test
  public void selectionIgnoresTablesWithoutServingInstances() {
    RoutingManager routing = routing(Map.of("a_OFFLINE", Set.of(), "b_OFFLINE", Set.of("s1")), Set.of("s1"));
    assertEquals(SingleConnectionBrokerRequestHandler.selectProbeTables(routing, 5), List.of("b_OFFLINE"));
  }

  /// When maxTables caps the set-cover below the tables needed to span the servers, the leftover servers
  /// are reported as uncovered so the operator can raise maxTables.
  @Test
  public void coverageDetectsUncoveredServersWhenMaxTablesTooSmall() {
    RoutingManager routing = routing(
        Map.of("a_OFFLINE", Set.of("s1"), "b_OFFLINE", Set.of("s2"), "c_OFFLINE", Set.of("s3"),
            "d_OFFLINE", Set.of("s4")),
        Set.of("s1", "s2", "s3", "s4"));

    List<String> selected = SingleConnectionBrokerRequestHandler.selectProbeTables(routing, 2);
    assertEquals(selected.size(), 2);
    // Sorted greedy picks a_OFFLINE (s1) and b_OFFLINE (s2); s3 and s4 are left unprobed.
    assertEquals(SingleConnectionBrokerRequestHandler.uncoveredRoutableServers(routing, selected),
        Set.of("s3", "s4"));
  }

  /// A maxTables large enough for the set-cover to span every server leaves nothing uncovered.
  @Test
  public void coverageIsCompleteWhenMaxTablesSpansServers() {
    RoutingManager routing = routing(
        Map.of("a_OFFLINE", Set.of("s1"), "b_OFFLINE", Set.of("s2"), "c_OFFLINE", Set.of("s3"),
            "d_OFFLINE", Set.of("s4")),
        Set.of("s1", "s2", "s3", "s4"));

    List<String> selected = SingleConnectionBrokerRequestHandler.selectProbeTables(routing, 5);
    assertTrue(SingleConnectionBrokerRequestHandler.uncoveredRoutableServers(routing, selected).isEmpty());
  }

  /// A custom warmup.tables list that omits a server surfaces that server as uncovered.
  @Test
  public void coverageDetectsServersMissingFromCustomTableList() {
    RoutingManager routing = routing(
        Map.of("a_OFFLINE", Set.of("s1"), "b_OFFLINE", Set.of("s2")), Set.of("s1", "s2"));

    assertEquals(SingleConnectionBrokerRequestHandler.uncoveredRoutableServers(routing, List.of("a_OFFLINE")),
        Set.of("s2"));
  }

  /// At concurrency 1 the round-robin must advance across rounds (startSeq monotonic) so successive batches
  /// walk every item and wrap. A per-round reset would probe only items.get(0) forever.
  @Test
  public void roundRobinBatchWalksAllItemsAtConcurrencyOne() {
    List<String> items = List.of("a", "b", "c");
    assertEquals(SingleConnectionBrokerRequestHandler.roundRobinBatch(items, 0, 1), List.of("a"));
    assertEquals(SingleConnectionBrokerRequestHandler.roundRobinBatch(items, 1, 1), List.of("b"));
    assertEquals(SingleConnectionBrokerRequestHandler.roundRobinBatch(items, 2, 1), List.of("c"));
    assertEquals(SingleConnectionBrokerRequestHandler.roundRobinBatch(items, 3, 1), List.of("a"));
  }

  /// A concurrency batch wraps around the item list, and an overflowed (negative) startSeq still yields a
  /// valid in-range index rather than throwing (Math.floorMod).
  @Test
  public void roundRobinBatchWrapsAndSurvivesOverflow() {
    List<String> items = List.of("a", "b", "c");
    assertEquals(SingleConnectionBrokerRequestHandler.roundRobinBatch(items, 0, 2), List.of("a", "b"));
    assertEquals(SingleConnectionBrokerRequestHandler.roundRobinBatch(items, 2, 2), List.of("c", "a"));
    // Batch larger than the list repeats items.
    assertEquals(SingleConnectionBrokerRequestHandler.roundRobinBatch(items, 0, 4), List.of("a", "b", "c", "a"));
    // startSeq + i overflows int on the second element; must not throw and must stay in range.
    assertEquals(SingleConnectionBrokerRequestHandler.roundRobinBatch(items, Integer.MAX_VALUE, 2).size(), 2);
  }

  /// reportServerCoverage records the uncovered-server count as a gauge: 2 when only one of three servers
  /// is probed, 1 when a server has no table routing to it, 0 when the tables span every server.
  @Test
  public void reportServerCoverageRecordsUncoveredGauge() {
    // s3 is in the cluster-wide server map but NO table routes to it -- another tenant's server. The
    // coverage universe is the servers THIS broker routes to (union of serving instances over its routable
    // tables), so s3 must NOT be counted as uncovered.
    RoutingManager routing = routing(
        Map.of("a_OFFLINE", Set.of("s1"), "b_OFFLINE", Set.of("s2")), Set.of("s1", "s2", "s3"));
    BrokerMetrics metrics = mock(BrokerMetrics.class);

    SingleConnectionBrokerRequestHandler.reportServerCoverage(routing, metrics, false, List.of("a_OFFLINE"));
    // Only s2 uncovered; s3 (no routing) is not this broker's server and is excluded.
    verify(metrics).setValueOfGlobalGauge(BrokerGauge.BROKER_WARMUP_UNCOVERED_SERVERS, 1L);

    SingleConnectionBrokerRequestHandler.reportServerCoverage(routing, metrics, false,
        List.of("a_OFFLINE", "b_OFFLINE"));
    verify(metrics).setValueOfGlobalGauge(BrokerGauge.BROKER_WARMUP_UNCOVERED_SERVERS, 0L);

    RoutingManager full = routing(Map.of("a_OFFLINE", Set.of("s1", "s2")), Set.of("s1", "s2"));
    BrokerMetrics fullMetrics = mock(BrokerMetrics.class);
    SingleConnectionBrokerRequestHandler.reportServerCoverage(full, fullMetrics, false, List.of("a_OFFLINE"));
    verify(fullMetrics).setValueOfGlobalGauge(BrokerGauge.BROKER_WARMUP_UNCOVERED_SERVERS, 0L);
  }

  /// The offline/realtime/hybrid routing decision: a raw name (tableType null) routes every type that
  /// exists; a type-suffixed name routes only that type; a type with no matching table routes nothing.
  /// This is the exact matrix that broke the raw-name custom-query probe (both legs set on an offline-only
  /// table).
  @Test
  public void probeRoutingDecisionCoversOfflineRealtimeHybrid() {
    TableRouteInfo offlineOnly = route(true, false);
    TableRouteInfo realtimeOnly = route(false, true);
    TableRouteInfo hybrid = route(true, true);

    assertTrue(SingleConnectionBrokerRequestHandler.shouldRouteOffline(null, offlineOnly));
    assertFalse(SingleConnectionBrokerRequestHandler.shouldRouteRealtime(null, offlineOnly));
    assertFalse(SingleConnectionBrokerRequestHandler.shouldRouteOffline(null, realtimeOnly));
    assertTrue(SingleConnectionBrokerRequestHandler.shouldRouteRealtime(null, realtimeOnly));
    assertTrue(SingleConnectionBrokerRequestHandler.shouldRouteOffline(null, hybrid));
    assertTrue(SingleConnectionBrokerRequestHandler.shouldRouteRealtime(null, hybrid));

    assertTrue(SingleConnectionBrokerRequestHandler.shouldRouteOffline(TableType.OFFLINE, hybrid));
    assertFalse(SingleConnectionBrokerRequestHandler.shouldRouteRealtime(TableType.OFFLINE, hybrid));
    assertFalse(SingleConnectionBrokerRequestHandler.shouldRouteOffline(TableType.REALTIME, hybrid));
    assertTrue(SingleConnectionBrokerRequestHandler.shouldRouteRealtime(TableType.REALTIME, hybrid));
    // A realtime-typed query against an offline-only table routes nothing.
    assertFalse(SingleConnectionBrokerRequestHandler.shouldRouteOffline(TableType.REALTIME, offlineOnly));
    assertFalse(SingleConnectionBrokerRequestHandler.shouldRouteRealtime(TableType.REALTIME, offlineOnly));
  }

  /// A custom `warmup.tables` list keeps only tables this broker routes, dropping the rest.
  @Test
  public void filterRoutableTablesKeepsOnlyRoutable() {
    RoutingManager routing =
        routing(Map.of("a_OFFLINE", Set.of("s1"), "b_OFFLINE", Set.of("s2")), Set.of("s1", "s2"));
    assertEquals(SingleConnectionBrokerRequestHandler.filterRoutableTables(routing,
        List.of("a_OFFLINE", "missing_OFFLINE", "b_OFFLINE")), List.of("a_OFFLINE", "b_OFFLINE"));
    assertTrue(
        SingleConnectionBrokerRequestHandler.filterRoutableTables(routing, List.of("nope_OFFLINE")).isEmpty());
  }


  private static TableRouteInfo route(boolean hasOffline, boolean hasRealtime) {
    TableRouteInfo routeInfo = mock(TableRouteInfo.class);
    when(routeInfo.hasOffline()).thenReturn(hasOffline);
    when(routeInfo.hasRealtime()).thenReturn(hasRealtime);
    return routeInfo;
  }

  /// The coverage universe is tenant-scoped: the union of serving instances over the broker's routable
  /// tables, NOT the cluster-wide getRoutableServerInstanceMap(). A server no routable table routes to
  /// (another tenant's server) is excluded, so it is never picked as a cover target nor flagged uncovered.
  @Test
  public void routableServersIsTenantScopedNotClusterWide() {
    // s4 is in the cluster-wide server map but no table routes to it.
    RoutingManager routing = routing(
        Map.of("a_OFFLINE", Set.of("s1", "s2"), "b_OFFLINE", Set.of("s3")), Set.of("s1", "s2", "s3", "s4"));
    assertEquals(SingleConnectionBrokerRequestHandler.routableServers(routing), Set.of("s1", "s2", "s3"));
    // With every routable server covered by the two tables, nothing is uncovered even though s4 is in the
    // cluster-wide map.
    assertTrue(SingleConnectionBrokerRequestHandler.uncoveredRoutableServers(
        routing, List.of("a_OFFLINE", "b_OFFLINE")).isEmpty());
  }

  private static RoutingManager routing(Map<String, Set<String>> tableToServers, Set<String> routableServers) {
    RoutingManager routingManager = mock(RoutingManager.class);
    when(routingManager.getRoutableTables()).thenReturn(tableToServers.keySet());
    tableToServers.forEach((table, servers) -> when(routingManager.getServingInstances(table)).thenReturn(servers));
    Map<String, ServerInstance> serverMap = new HashMap<>();
    for (String server : routableServers) {
      serverMap.put(server, mock(ServerInstance.class));
    }
    when(routingManager.getRoutableServerInstanceMap()).thenReturn(serverMap);
    return routingManager;
  }
}

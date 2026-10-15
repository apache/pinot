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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
/// knobs and their floors), the no-op interface default, the greedy table selection that always covers every
/// routable server, the static-probe routing decision, the round-robin rotation, the server-coverage gauge,
/// and the concurrent-round budget semantics.
public class BrokerWarmupTest {

  @Test
  public void warmupIsDisabledByDefault() {
    BrokerWarmupConfig config = BrokerWarmupConfig.from(new PinotConfiguration());
    assertFalse(config.enabled());
    // Defaults still parse, so flipping the flag alone yields sane behavior.
    assertEquals(config.budgetMs(), Broker.DEFAULT_BROKER_STARTUP_WARMUP_BUDGET_MS);
    assertEquals(config.minIterations(), Broker.DEFAULT_BROKER_STARTUP_WARMUP_MIN_ITERATIONS);
    // Concurrency defaults to serial.
    assertEquals(config.concurrency(), Broker.DEFAULT_BROKER_STARTUP_WARMUP_CONCURRENCY);
    assertEquals(config.concurrency(), 1);
  }

  @Test
  public void configIsReadFromProperties() {
    PinotConfiguration properties = new PinotConfiguration();
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_ENABLED, true);
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_BUDGET_MS, 4321L);
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_MIN_ITERATIONS, 250);
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_CONCURRENCY, 12);

    BrokerWarmupConfig config = BrokerWarmupConfig.from(properties);
    assertTrue(config.enabled());
    assertEquals(config.budgetMs(), 4321L);
    assertEquals(config.minIterations(), 250);
    assertEquals(config.concurrency(), 12);
  }

  /// concurrency and minIterations are floored at 1 even if misconfigured to 0/negative, so the probe pool
  /// and the exit floor are always valid.
  @Test
  public void concurrencyAndMinIterationsFlooredAtOne() {
    PinotConfiguration properties = new PinotConfiguration();
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_CONCURRENCY, 0);
    properties.setProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_MIN_ITERATIONS, -5);
    BrokerWarmupConfig config = BrokerWarmupConfig.from(properties);
    assertEquals(config.concurrency(), 1);
    assertEquals(config.minIterations(), 1);
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

  /// A handler that does not override warmUp must report warm immediately. If the default blocked or
  /// returned false, every non-single-stage handler would hold readiness shut forever.
  @Test
  public void defaultWarmUpIsANoOp() {
    BrokerRequestHandler handler = mock(BrokerRequestHandler.class);
    when(handler.warmUp(any(), anyLong())).thenCallRealMethod();
    assertTrue(handler.warmUp(new BrokerWarmupConfig(true, 1L, 1, 1),
        System.currentTimeMillis() + 1_000L));
  }

  /// Three tables, three servers, one table per server: all three must be picked, since dropping any one
  /// leaves a broker-to-server channel unwarmed.
  @Test
  public void selectionCoversEveryServer() {
    RoutingManager routing = routing(
        Map.of("a_OFFLINE", Set.of("s1"), "b_OFFLINE", Set.of("s2"), "c_OFFLINE", Set.of("s3")),
        Set.of("s1", "s2", "s3"));

    assertEquals(new java.util.HashSet<>(SingleConnectionBrokerRequestHandler.selectProbeTables(routing)),
        Set.of("a_OFFLINE", "b_OFFLINE", "c_OFFLINE"));
  }

  /// A table adding no new server is skipped: probing it costs a query and warms nothing extra.
  @Test
  public void selectionSkipsRedundantTables() {
    RoutingManager routing = routing(
        Map.of("a_OFFLINE", Set.of("s1", "s2"), "b_OFFLINE", Set.of("s1"), "c_OFFLINE", Set.of("s2")),
        Set.of("s1", "s2"));

    // "a_OFFLINE" sorts first and already covers both servers, so nothing else is needed.
    assertEquals(SingleConnectionBrokerRequestHandler.selectProbeTables(routing), List.of("a_OFFLINE"));
  }

  /// The one, only behavior: the set-cover always spans EVERY routable server, however many tables that
  /// needs (there is no cap). The loop still stops at full coverage, so it picks exactly one table per
  /// server here and no more.
  @Test
  public void selectionCoversWideFleet() {
    // Seven servers, each served by exactly one distinct table: covering all of them needs all seven tables.
    RoutingManager routing = routing(
        Map.of("a_OFFLINE", Set.of("s1"), "b_OFFLINE", Set.of("s2"), "c_OFFLINE", Set.of("s3"),
            "d_OFFLINE", Set.of("s4"), "e_OFFLINE", Set.of("s5"), "f_OFFLINE", Set.of("s6"),
            "g_OFFLINE", Set.of("s7")),
        Set.of("s1", "s2", "s3", "s4", "s5", "s6", "s7"));

    List<String> selected = SingleConnectionBrokerRequestHandler.selectProbeTables(routing);
    assertEquals(selected.size(), 7);
    assertTrue(SingleConnectionBrokerRequestHandler.uncoveredRoutableServers(routing, selected).isEmpty());
  }

  /// Warmup runs before the first query and may legitimately find nothing routable yet.
  @Test
  public void selectionHandlesEmptyCluster() {
    assertTrue(SingleConnectionBrokerRequestHandler.selectProbeTables(routing(Map.of(), Set.of())).isEmpty());
  }

  /// Must not blow up on a table whose serving-instance set is empty or unknown.
  @Test
  public void selectionIgnoresTablesWithoutServingInstances() {
    RoutingManager routing = routing(Map.of("a_OFFLINE", Set.of(), "b_OFFLINE", Set.of("s1")), Set.of("s1"));
    assertEquals(SingleConnectionBrokerRequestHandler.selectProbeTables(routing), List.of("b_OFFLINE"));
  }

  /// The auto-select set-cover always leaves nothing uncovered -- every routable server is spanned.
  @Test
  public void coverageIsCompleteForAutoSelect() {
    RoutingManager routing = routing(
        Map.of("a_OFFLINE", Set.of("s1"), "b_OFFLINE", Set.of("s2"), "c_OFFLINE", Set.of("s3"),
            "d_OFFLINE", Set.of("s4")),
        Set.of("s1", "s2", "s3", "s4"));

    List<String> selected = SingleConnectionBrokerRequestHandler.selectProbeTables(routing);
    assertTrue(SingleConnectionBrokerRequestHandler.uncoveredRoutableServers(routing, selected).isEmpty());
  }

  /// uncoveredRoutableServers surfaces the servers a given table set misses: probing only a_OFFLINE (which
  /// serves s1) leaves s2 uncovered.
  @Test
  public void uncoveredRoutableServersIdentifiesMissedServers() {
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

  /// An empty item list must yield an empty batch, not an ArithmeticException from `floorMod(x, 0)`. Callers
  /// guard today, but the helper must be safe for a future caller.
  @Test
  public void roundRobinBatchOnEmptyListIsEmpty() {
    assertTrue(SingleConnectionBrokerRequestHandler.roundRobinBatch(List.of(), 0, 4).isEmpty());
    assertTrue(SingleConnectionBrokerRequestHandler.roundRobinBatch(List.<String>of(), 7, 1).isEmpty());
  }

  /// reportServerCoverage records, at exit, the servers left cold given the tables actually probed: 1 when
  /// only one of two servers' tables was probed (the other missed, e.g. budget expired first), 0 when every
  /// server's table was probed. A cluster server no routable table routes to is excluded from the universe.
  @Test
  public void reportServerCoverageRecordsUncoveredGauge() {
    // s3 is in the cluster-wide server map but NO table routes to it -- another tenant's server. The
    // coverage universe is the servers THIS broker routes to (union of serving instances over its routable
    // tables), so s3 must NOT be counted as uncovered.
    RoutingManager routing = routing(
        Map.of("a_OFFLINE", Set.of("s1"), "b_OFFLINE", Set.of("s2")), Set.of("s1", "s2", "s3"));
    BrokerMetrics metrics = mock(BrokerMetrics.class);

    // Only a_OFFLINE was probed (b_OFFLINE never reached -- budget expired) -> s2 left cold; s3 excluded.
    SingleConnectionBrokerRequestHandler.reportServerCoverage(routing, metrics, List.of("a_OFFLINE"));
    verify(metrics).setValueOfGlobalGauge(BrokerGauge.STARTUP_WARMUP_UNCOVERED_SERVERS, 1L);

    // Both tables probed -> every server warmed.
    SingleConnectionBrokerRequestHandler.reportServerCoverage(routing, metrics,
        List.of("a_OFFLINE", "b_OFFLINE"));
    verify(metrics).setValueOfGlobalGauge(BrokerGauge.STARTUP_WARMUP_UNCOVERED_SERVERS, 0L);

    RoutingManager full = routing(Map.of("a_OFFLINE", Set.of("s1", "s2")), Set.of("s1", "s2"));
    BrokerMetrics fullMetrics = mock(BrokerMetrics.class);
    SingleConnectionBrokerRequestHandler.reportServerCoverage(full, fullMetrics, List.of("a_OFFLINE"));
    verify(fullMetrics).setValueOfGlobalGauge(BrokerGauge.STARTUP_WARMUP_UNCOVERED_SERVERS, 0L);
  }

  /// The offline/realtime/hybrid routing decision the probe uses: a type-suffixed name routes only that
  /// type; a type with no matching table routes nothing. (The untyped/`null` cases are covered too, as a
  /// general contract of the helper, though the static probe always supplies a type-suffixed name.)
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

  /// A round collects the latencies of probes that completed with a non-negative value, and silently drops
  /// a probe that failed (returned -1) or threw. This is the "everything finishes within budget" happy path.
  @Test
  public void runConcurrentRoundCollectsSuccessfulLatenciesOnly() {
    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      long deadline = System.currentTimeMillis() + 5_000L;
      List<Callable<Long>> tasks = List.of(
          () -> 10L,
          () -> -1L,                                       // failed probe: excluded
          () -> 20L,
          () -> {
            throw new RuntimeException("probe blew up");   // threw: excluded, must not fail the round
          });
      List<Long> latencies =
          SingleConnectionBrokerRequestHandler.runConcurrentRound(tasks, pool, deadline, new boolean[1]);
      assertEquals(new HashSet<>(latencies), Set.of(10L, 20L));
      assertEquals(latencies.size(), 2);
    } finally {
      pool.shutdownNow();
    }
  }

  /// The budget is a hard ceiling even when more probes are submitted than the pool has threads: probes 2
  /// and 3 queue behind probe 1, and every probe sleeps far past the budget. The round must return at ~the
  /// budget (never sum-of-sleeps), collect nothing, and not leave the queued probes to run -- the exact
  /// overshoot that #1/#2 fixed.
  @Test
  public void runConcurrentRoundReturnsByBudgetWhenTasksQueue() {
    ExecutorService pool = Executors.newFixedThreadPool(1);
    try {
      AtomicInteger started = new AtomicInteger(0);
      List<Callable<Long>> tasks = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        tasks.add(() -> {
          started.incrementAndGet();
          Thread.sleep(5_000L);
          return 1L;
        });
      }
      long deadline = System.currentTimeMillis() + 300L;
      long start = System.currentTimeMillis();
      List<Long> latencies =
          SingleConnectionBrokerRequestHandler.runConcurrentRound(tasks, pool, deadline, new boolean[1]);
      long elapsed = System.currentTimeMillis() - start;
      assertTrue(elapsed < 3_000L, "round must return near the 300ms budget, took " + elapsed + "ms");
      assertTrue(latencies.isEmpty(), "no 5s probe can finish within a 300ms budget");
      // Only the head-of-queue probe ever ran; the two queued behind it were cancelled, not fired. (<=2
      // rather than ==1 only to tolerate the microsecond window where probe 1's interrupt frees the single
      // pool thread before cancelAll marks probe 2 cancelled.)
      assertTrue(started.get() <= 2, "queued probes must not run past the budget, started=" + started.get());
    } finally {
      pool.shutdownNow();
    }
  }

  /// A deadline already in the past collects nothing and returns at once -- it never waits on a submitted
  /// probe. Guards the loop's leading remaining-budget check.
  @Test
  public void runConcurrentRoundWithExpiredDeadlineCollectsNothing() {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      List<Callable<Long>> tasks = List.of(() -> 1L, () -> 2L);
      List<Long> latencies = SingleConnectionBrokerRequestHandler.runConcurrentRound(tasks, pool,
          System.currentTimeMillis() - 1L, new boolean[1]);
      assertTrue(latencies.isEmpty());
    } finally {
      pool.shutdownNow();
    }
  }

  /// Interrupting the thread running the round (shutdown mid-warmup) must end it promptly -- not wait out the
  /// in-flight probes -- return what it had, and preserve the interrupt status so the warmup loop above it
  /// sees the interrupt and stops.
  @Test
  public void runConcurrentRoundReturnsPromptlyWhenInterrupted() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      List<Callable<Long>> tasks = List.of(
          () -> {
            Thread.sleep(10_000L);
            return 1L;
          },
          () -> {
            Thread.sleep(10_000L);
            return 2L;
          });
      // Deadline far off, so only the interrupt -- not the budget -- can end the round.
      long deadline = System.currentTimeMillis() + 30_000L;
      AtomicReference<List<Long>> result = new AtomicReference<>();
      AtomicBoolean interruptPreserved = new AtomicBoolean(false);
      Thread runner = new Thread(() -> {
        List<Long> r = SingleConnectionBrokerRequestHandler.runConcurrentRound(tasks, pool, deadline, new boolean[1]);
        result.set(r);
        interruptPreserved.set(Thread.currentThread().isInterrupted());
      });
      runner.start();
      Thread.sleep(200L);   // let it enter Future.get()
      runner.interrupt();
      runner.join(3_000L);
      assertFalse(runner.isAlive(), "interrupt must end the round promptly, not wait 10s for the probes");
      assertTrue(result.get().isEmpty());
      assertTrue(interruptPreserved.get(), "interrupt status must be re-set for the caller loop to observe");
    } finally {
      pool.shutdownNow();
    }
  }

  /// The probe-pool drain must actually wait even when the caller's interrupt flag is set -- the state
  /// shutdown leaves the warmup thread in. A busy task that ignores interrupts stands in for a probe
  /// mid-flight: a naive `awaitTermination` would throw immediately on the interrupt flag and skip the
  /// drain, leaving the pool un-terminated; the save-and-clear must let the wait complete and restore the
  /// flag. This is the shutdown path #17 was about.
  @Test
  public void awaitPoolDrainWaitsForTasksEvenWhenCallerInterrupted() throws Exception {
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch started = new CountDownLatch(1);
      pool.submit(() -> {
        started.countDown();
        long end = System.currentTimeMillis() + 300L;
        while (System.currentTimeMillis() < end) {
          // Busy-wait that deliberately ignores interrupts, standing in for a probe mid-flight.
        }
      });
      assertTrue(started.await(2, TimeUnit.SECONDS));
      pool.shutdownNow();                   // interrupts the (interrupt-ignoring) task
      Thread.currentThread().interrupt();   // as stopWarmup() leaves the warmup thread on shutdown

      SingleConnectionBrokerRequestHandler.awaitPoolDrain(pool, 5_000L);

      // Flag restored (and cleared here so it does not leak to other tests); and the drain actually waited
      // for the task rather than returning 0ms on the interrupt flag.
      assertTrue(Thread.interrupted(), "the interrupt flag must be restored");
      assertTrue(pool.isTerminated(), "drain must wait for the task, not skip on the interrupt flag");
    } finally {
      pool.shutdownNow();
    }
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

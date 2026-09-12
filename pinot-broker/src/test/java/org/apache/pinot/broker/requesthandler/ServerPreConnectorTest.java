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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.broker.requesthandler.ServerPreConnector.ChannelTarget;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ServerPreConnectorTest {
  private static final long ONE_MINUTE_MS = 60_000L;

  private static List<ServerInstance> mockServers(int count) {
    List<ServerInstance> servers = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      servers.add(mock(ServerInstance.class));
    }
    return servers;
  }

  /// The (server, table type) channels the caller would derive from routing: here just the cross product
  /// of the given servers and types, so the connector-behaviour tests can exercise a known channel count.
  private static List<ChannelTarget> targets(List<ServerInstance> servers, TableType... types) {
    List<ChannelTarget> targets = new ArrayList<>(servers.size() * types.length);
    for (ServerInstance server : servers) {
      for (TableType type : types) {
        targets.add(new ChannelTarget(server, type));
      }
    }
    return targets;
  }

  private static long farDeadline() {
    return System.currentTimeMillis() + ONE_MINUTE_MS;
  }

  @Test
  public void connectsEverySuppliedTarget() {
    List<ServerInstance> servers = mockServers(3);
    Set<TableType> tableTypesSeen = ConcurrentHashMap.newKeySet();
    Set<Integer> serversSeen = ConcurrentHashMap.newKeySet();
    AtomicInteger calls = new AtomicInteger();

    int connected = new ServerPreConnector(() -> targets(servers, TableType.OFFLINE, TableType.REALTIME),
        (server, tableType, timeoutMs) -> {
          calls.incrementAndGet();
          tableTypesSeen.add(tableType);
          serversSeen.add(System.identityHashCode(server));
          return true;
        }).preConnect(farDeadline());

    // 3 servers x 2 table types.
    assertEquals(connected, 6);
    assertEquals(calls.get(), 6);
    assertEquals(serversSeen.size(), 3);
    assertEquals(tableTypesSeen, EnumSet.of(TableType.OFFLINE, TableType.REALTIME));
  }

  @Test
  public void emptyTargetsReturnsZeroWithoutConnecting() {
    AtomicInteger calls = new AtomicInteger();
    int connected = new ServerPreConnector(List::of, (server, tableType, timeoutMs) -> {
      calls.incrementAndGet();
      return true;
    }).preConnect(farDeadline());

    assertEquals(connected, 0);
    assertEquals(calls.get(), 0);
  }

  @Test
  public void deadlineAlreadyPassedReturnsZeroWithoutConnecting() {
    List<ServerInstance> servers = mockServers(2);
    AtomicInteger calls = new AtomicInteger();
    int connected = new ServerPreConnector(() -> targets(servers, TableType.OFFLINE, TableType.REALTIME),
        (server, tableType, timeoutMs) -> {
          calls.incrementAndGet();
          return true;
        }).preConnect(System.currentTimeMillis() - 1);

    assertEquals(connected, 0);
    assertEquals(calls.get(), 0);
  }

  @Test
  public void countsOnlySuccessfulConnects() {
    List<ServerInstance> servers = mockServers(4);
    // OFFLINE succeeds, REALTIME fails: exactly one successful channel per server.
    int connected = new ServerPreConnector(() -> targets(servers, TableType.OFFLINE, TableType.REALTIME),
        (server, tableType, timeoutMs) -> tableType == TableType.OFFLINE).preConnect(farDeadline());

    assertEquals(connected, 4);
  }

  @Test
  public void connectFailureIsSwallowedAndOthersStillConnect() {
    List<ServerInstance> servers = mockServers(5);
    AtomicInteger attempts = new AtomicInteger();
    // Every REALTIME attempt throws; the method must not propagate it and must still connect OFFLINE.
    int connected = new ServerPreConnector(() -> targets(servers, TableType.OFFLINE, TableType.REALTIME),
        (server, tableType, timeoutMs) -> {
          attempts.incrementAndGet();
          if (tableType == TableType.REALTIME) {
            throw new RuntimeException("connect blew up");
          }
          return true;
        }).preConnect(farDeadline());

    assertEquals(connected, 5);        // only the 5 OFFLINE channels
    assertEquals(attempts.get(), 10);  // all 10 were still attempted
  }

  @Test
  public void respectsBudgetAndDoesNotWaitForSlowConnects() {
    List<ServerInstance> servers = mockServers(4);
    // Every connect is far slower than the budget; preConnect must return near the budget, not wait for
    // the connects, and must not throw. Nothing completes, so the first poll (which gets the whole budget)
    // returns empty and releases.
    long budgetMs = 400L;
    long startMs = System.currentTimeMillis();
    int connected = new ServerPreConnector(() -> targets(servers, TableType.OFFLINE, TableType.REALTIME),
        (server, tableType, timeoutMs) -> {
          try {
            Thread.sleep(5_000L);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
          }
          return true;
        }).preConnect(System.currentTimeMillis() + budgetMs);
    long elapsedMs = System.currentTimeMillis() - startMs;

    assertEquals(connected, 0);
    // Comfortably below the 5s connect: proves the budget bounded the wait rather than blocking on
    // the slow connects.
    assertTrue(elapsedMs < 3_000L, "preConnect took " + elapsedMs + " ms, expected it to honor the budget");
  }

  /// An unreachable server that black-holes (never completes) holds pre-connect only until the deadline,
  /// and the healthy channels are still counted. There is no early release: the deadline is the single
  /// bound, so a broker with one dead server pays at most the budget on startup, and that cost is tuned
  /// through the budget rather than a heuristic.
  @Test
  public void blackHoledChannelIsBoundedByTheDeadlineAndHealthyChannelsAreCounted() {
    List<ServerInstance> servers = mockServers(4);   // 4 x 2 table types = 8 channels
    long budgetMs = 800L;
    AtomicInteger n = new AtomicInteger();
    long startMs = System.currentTimeMillis();

    int connected = new ServerPreConnector(() -> targets(servers, TableType.OFFLINE, TableType.REALTIME),
        (server, tableType, timeoutMs) -> {
          if (n.getAndIncrement() == 0) {
            try {
              Thread.sleep(5_000L);   // black-hole well past the budget; never connects in time
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return false;
          }
          return true;
        }).preConnect(startMs + budgetMs);
    long elapsedMs = System.currentTimeMillis() - startMs;

    assertEquals(connected, 7, "the seven healthy channels must still be counted");
    // Bounded by the deadline: the one black-holed channel holds only until the budget, never longer.
    assertTrue(elapsedMs < 3 * budgetMs,
        "one black-holed channel held startup for " + elapsedMs + " ms of an " + budgetMs + " ms budget");
  }

  /// Slow-but-healthy channels are all counted: with the deadline as the only bound, a cluster whose every
  /// channel is slow (but faster than the budget) still connects every one and is released once they are
  /// all up -- not at the deadline, and never with any of them abandoned.
  @Test
  public void slowHealthyChannelsAreAllCounted() {
    List<ServerInstance> servers = mockServers(3);      // 3 x 2 = 6 channels
    long slowMs = 2_500L;                               // slow, but faster than the budget
    long budgetMs = 30_000L;
    long startMs = System.currentTimeMillis();

    int connected = new ServerPreConnector(() -> targets(servers, TableType.OFFLINE, TableType.REALTIME),
        (server, tableType, timeoutMs) -> {
          try {
            Thread.sleep(slowMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
          }
          return true;
        }).preConnect(startMs + budgetMs);
    long elapsedMs = System.currentTimeMillis() - startMs;

    assertEquals(connected, 6, "every channel must be counted even though all are slow");
    assertTrue(elapsedMs >= slowMs, "the slow channels must be waited for, not abandoned");
    assertTrue(elapsedMs < budgetMs, "must be released once the channels are back, not held to the deadline");
  }

  /// The core invariant: a failed connect must not stop us waiting for the others. One connect fails
  /// instantly and completes first; the five healthy-but-slower channels must still all be waited for and
  /// counted. (Under the old grace window, keying the release on the first *completion* undercounted this
  /// to 0; waiting to the deadline makes it unconditional.)
  @Test
  public void aFastFailureDoesNotStopWaitingForTheOtherChannels() {
    List<ServerInstance> servers = mockServers(3);      // 3 x 2 = 6 channels
    long slowMs = 2_500L;                               // slow, but faster than the budget
    long budgetMs = 30_000L;
    AtomicInteger n = new AtomicInteger();

    int connected = new ServerPreConnector(() -> targets(servers, TableType.OFFLINE, TableType.REALTIME),
        (server, tableType, timeoutMs) -> {
          if (n.getAndIncrement() == 0) {
            return false;   // one instant failure, completes first, must not end the wait
          }
          try {
            Thread.sleep(slowMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
          }
          return true;
        }).preConnect(System.currentTimeMillis() + budgetMs);

    assertEquals(connected, 5,
        "the five healthy channels must be counted; a fast failure must not stop us waiting for them");
  }

  /// More channels than worker threads: the surplus queues behind the pool and still all connect. Exercises
  /// the `min(channelCount, MAX_CONNECT_THREADS)` pool sizing and the queue draining that the completion
  /// loop depends on.
  @Test
  public void moreTargetsThanThreadsAllConnect() {
    int count = ServerPreConnector.MAX_CONNECT_THREADS * 3;   // 48 servers -> 96 channels, pool caps at 16
    List<ServerInstance> servers = mockServers(count);
    AtomicInteger calls = new AtomicInteger();

    int connected = new ServerPreConnector(() -> targets(servers, TableType.OFFLINE, TableType.REALTIME),
        (server, tableType, timeoutMs) -> {
          calls.incrementAndGet();
          return true;
        }).preConnect(farDeadline());

    assertEquals(connected, count * 2, "every queued channel must eventually connect");
    assertEquals(calls.get(), count * 2, "every channel must be attempted");
  }

  /// More channels than worker threads, every one HEALTHY but slow. The surplus completes in waves one
  /// connect-latency apart; because pre-connect waits to the deadline rather than releasing on a quiet
  /// window, every wave is waited for and all connect. (The grace window this replaces under-counted this
  /// to ~one wave when the inter-wave gap exceeded the window.)
  @Test
  public void manyHealthyChannelsInWavesAllConnect() {
    int count = ServerPreConnector.MAX_CONNECT_THREADS * 3;   // 48 channels, pool caps at 16 -> 3 waves
    List<ServerInstance> servers = mockServers(count);
    long slowMs = 3_000L;

    int connected = new ServerPreConnector(() -> targets(servers, TableType.OFFLINE),
        (server, tableType, timeoutMs) -> {
          try {
            Thread.sleep(slowMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
          }
          return true;
        }).preConnect(System.currentTimeMillis() + 60_000L);

    assertEquals(connected, count, "all healthy channels must connect even when they complete in waves");
  }

  /// Mixed connect latencies -- one fast server, the rest slower -- now connect **all** channels. The old
  /// grace window sized itself off the first (fastest) connect and released before the slower healthy
  /// channels returned, counting only 1; waiting to the deadline waits for every one. Regression test for
  /// that mixed-latency under-count.
  @Test
  public void mixedLatencyAllChannelsConnect() {
    List<ServerInstance> servers = mockServers(8);   // 8 channels, all start at once on the 16-worker pool
    long slowMs = 3_000L;
    long budgetMs = 30_000L;
    AtomicInteger n = new AtomicInteger();

    int connected = new ServerPreConnector(() -> targets(servers, TableType.OFFLINE),
        (server, tableType, timeoutMs) -> {
          if (n.getAndIncrement() == 0) {
            return true;   // one fast connect; must not curtail waiting for the slower healthy ones
          }
          try {
            Thread.sleep(slowMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
          }
          return true;
        }).preConnect(System.currentTimeMillis() + budgetMs);

    assertEquals(connected, 8,
        "with the deadline as the only bound, a fast connect no longer abandons the slower healthy channels");
  }

  /// The thread pool is a throughput cap, not a safety bound, so each connect has to carry its own
  /// deadline-derived timeout. Without it a channel queued behind a stuck worker could outlive the
  /// budget entirely.
  @Test
  public void passesRemainingBudgetToEachConnect() {
    List<ServerInstance> servers = mockServers(2);
    long budgetMs = 5_000L;
    AtomicLong maxTimeoutSeen = new AtomicLong(Long.MIN_VALUE);
    AtomicLong minTimeoutSeen = new AtomicLong(Long.MAX_VALUE);

    int connected = new ServerPreConnector(() -> targets(servers, TableType.OFFLINE, TableType.REALTIME),
        (server, tableType, timeoutMs) -> {
          maxTimeoutSeen.accumulateAndGet(timeoutMs, Math::max);
          minTimeoutSeen.accumulateAndGet(timeoutMs, Math::min);
          return true;
        }).preConnect(System.currentTimeMillis() + budgetMs);

    assertEquals(connected, 4);
    assertTrue(maxTimeoutSeen.get() <= budgetMs,
        "connect timeout " + maxTimeoutSeen.get() + " ms must never exceed the budget " + budgetMs + " ms");
    assertTrue(minTimeoutSeen.get() >= 0, "connect timeout must never be negative");
  }

  /// A connector may legitimately be handed a zero timeout when the budget runs out mid-flight. It must
  /// be treated as "no budget", never as "wait forever" -- Netty reads
  /// `ChannelOption.CONNECT_TIMEOUT_MILLIS <= 0` as *no* connect timeout, so a zero leaking through to
  /// the bootstrap would park a worker indefinitely, which is the opposite of what the bound is for.
  @Test
  public void connectTimeoutIsNeverNegative() {
    List<ServerInstance> servers = mockServers(8);
    AtomicLong minTimeoutSeen = new AtomicLong(Long.MAX_VALUE);

    new ServerPreConnector(() -> targets(servers, TableType.OFFLINE, TableType.REALTIME),
        (server, tableType, timeoutMs) -> {
          minTimeoutSeen.accumulateAndGet(timeoutMs, Math::min);
          try {
            Thread.sleep(20L);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return true;
        }).preConnect(System.currentTimeMillis() + 50L);

    assertTrue(minTimeoutSeen.get() >= 0,
        "connect timeout " + minTimeoutSeen.get() + " ms must never be negative");
  }

  // ---- routing-derived target selection (SingleConnectionBrokerRequestHandler#routableChannelTargets) ----

  private static RoutingManager routing(Map<String, ServerInstance> serverInstanceMap,
      Map<String, Set<String>> tableToServingInstances) {
    RoutingManager routingManager = mock(RoutingManager.class);
    when(routingManager.getRoutableServerInstanceMap()).thenReturn(serverInstanceMap);
    when(routingManager.getRoutableTables()).thenReturn(tableToServingInstances.keySet());
    tableToServingInstances.forEach(
        (table, servingInstances) -> when(routingManager.getServingInstances(table)).thenReturn(servingInstances));
    return routingManager;
  }

  private static Map<String, ServerInstance> serverInstances(String... ids) {
    Map<String, ServerInstance> map = new HashMap<>();
    for (String id : ids) {
      map.put(id, mock(ServerInstance.class));
    }
    return map;
  }

  /// Only the (server, table type) pairs routing actually uses are targeted: a server gets a channel for
  /// each type that routes to it (s2 hybrid -> both), never the cross product, and a server no table routes
  /// to (s4) gets nothing -- even though it is in the cluster-wide `getRoutableServerInstanceMap()`.
  @Test
  public void routableTargetsAreDerivedFromRoutingNotCrossProduct() {
    Map<String, ServerInstance> serverMap = serverInstances("s1", "s2", "s3", "s4");
    RoutingManager routingManager = routing(serverMap, Map.of(
        "a_OFFLINE", Set.of("s1", "s2"),
        "b_REALTIME", Set.of("s2", "s3")));

    Set<ChannelTarget> targets =
        new HashSet<>(SingleConnectionBrokerRequestHandler.routableChannelTargets(routingManager));

    assertEquals(targets, Set.of(
        new ChannelTarget(serverMap.get("s1"), TableType.OFFLINE),
        new ChannelTarget(serverMap.get("s2"), TableType.OFFLINE),
        new ChannelTarget(serverMap.get("s2"), TableType.REALTIME),
        new ChannelTarget(serverMap.get("s3"), TableType.REALTIME)));
  }

  /// An offline-only cluster opens no REALTIME channels (the wasted-duplicate-socket case).
  @Test
  public void offlineOnlyClusterTargetsNoRealtimeChannels() {
    Map<String, ServerInstance> serverMap = serverInstances("s1", "s2");
    RoutingManager routingManager = routing(serverMap, Map.of(
        "a_OFFLINE", Set.of("s1", "s2"),
        "b_OFFLINE", Set.of("s1")));

    Set<ChannelTarget> targets =
        new HashSet<>(SingleConnectionBrokerRequestHandler.routableChannelTargets(routingManager));

    // Two tables on s1 dedupe to a single (s1, OFFLINE); no REALTIME anywhere.
    assertEquals(targets, Set.of(
        new ChannelTarget(serverMap.get("s1"), TableType.OFFLINE),
        new ChannelTarget(serverMap.get("s2"), TableType.OFFLINE)));
  }

  /// No routable tables -> no targets (a broker that converged with an empty routing table, e.g. a tenant
  /// with no tables yet). preConnect then connects nothing.
  @Test
  public void routableTargetsEmptyWhenNoRoutableTables() {
    RoutingManager routingManager = routing(serverInstances("s1", "s2"), Map.of());
    assertTrue(SingleConnectionBrokerRequestHandler.routableChannelTargets(routingManager).isEmpty());
  }

  /// A serving instance not present in the routable-server map (e.g. just disabled) is skipped rather than
  /// producing a null-server target, and a table with no routing is ignored.
  @Test
  public void routableTargetsSkipUnknownServersAndUnroutedTables() {
    Map<String, ServerInstance> serverMap = serverInstances("s1");
    Map<String, Set<String>> tables = new HashMap<>();
    tables.put("a_OFFLINE", Set.of("s1", "gone"));   // "gone" is not in the routable-server map
    tables.put("b_OFFLINE", null);                   // no routing yet
    RoutingManager routingManager = routing(serverMap, tables);

    Set<ChannelTarget> targets =
        new HashSet<>(SingleConnectionBrokerRequestHandler.routableChannelTargets(routingManager));

    assertEquals(targets, Set.of(new ChannelTarget(serverMap.get("s1"), TableType.OFFLINE)));
  }
}

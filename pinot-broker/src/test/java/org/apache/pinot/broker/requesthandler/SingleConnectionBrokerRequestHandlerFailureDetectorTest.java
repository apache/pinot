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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.failuredetector.FailureDetectorFactory;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.routing.ImplicitHybridTableRouteInfo;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.transport.QueryServer;
import org.apache.pinot.core.transport.QueryServerTestUtils;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.util.TestUtils;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// End-to-end coverage, over real sockets, of the broker removing a server whose node is gone or whose JVM is frozen.
///
/// Such a server keeps its TCP connection open and never answers, so it produces query timeouts and nothing else: no
/// send exception, no channel teardown. Before this change the broker kept routing queries to it until Helix noticed,
/// minutes later, while healthy replicas sat idle. Now a timed-out query makes the broker ping the server, and a server
/// that does not answer the ping is taken out of routing.
///
/// These tests drive the real [SingleConnectionBrokerRequestHandler] and a real [FailureDetector] against real
/// servers, so the whole chain -- timeout, ping, routing exclusion, retry -- runs over real sockets.
public class SingleConnectionBrokerRequestHandlerFailureDetectorTest {
  private static final BrokerRequest BROKER_REQUEST =
      CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final long REQUEST_ID = 123L;
  private static final long QUERY_TIMEOUT_MS = 1_000L;
  private static final long PING_TIMEOUT_MS = 500L;
  /// Long enough that the retry loop never runs during a test that is not about it.
  private static final long RETRIES_OFF_MS = 600_000L;
  private static final long FAST_RETRY_DELAY_MS = 100L;

  private final List<QueryServer> _queryServers = new ArrayList<>();
  private final List<ServerSocket> _frozenServers = new ArrayList<>();
  /// What the routing manager reports as enabled. The ping-based check treats a server missing from here as
  /// unhealthy, so every server a test starts must be in it.
  private final Map<String, ServerInstance> _enabledServers = new ConcurrentHashMap<>();

  private BrokerRoutingManager _routingManager;
  private SingleConnectionBrokerRequestHandler _requestHandler;
  private FailureDetector _failureDetector;
  private List<String> _excludedServers;
  private List<String> _reincludedServers;

  @BeforeMethod
  public void setUp() {
    startBroker(true, RETRIES_OFF_MS);
  }

  /// Builds the broker-side stack under test: a real request handler wired to a real failure detector whose notifiers
  /// are recorded, mirroring the wiring `BaseBrokerStarter` performs in production. Without `pingOnTimeout` the
  /// default applies.
  private void startBroker(boolean pingOnTimeout, long retryInitialDelayMs) {
    // BrokerMetrics is a JVM-wide singleton, so the gauge carries over from whatever ran before in this fork.
    BrokerMetrics.get().setValueOfGlobalGauge(BrokerGauge.UNHEALTHY_SERVERS, 0);

    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Broker.FailureDetector.CONFIG_OF_TYPE, Broker.FailureDetector.Type.CONNECTION.name());
    if (pingOnTimeout) {
      config.setProperty(Broker.FailureDetector.CONFIG_OF_ENABLE_PING_ON_TIMEOUT, true);
    }
    config.setProperty(Broker.FailureDetector.CONFIG_OF_PING_TIMEOUT_MS, PING_TIMEOUT_MS);
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_INITIAL_DELAY_MS, retryInitialDelayMs);
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_FACTOR, 1);
    // Past the max retries a server is put back regardless, which would let a recovery test pass with no ping answered.
    config.setProperty(Broker.FailureDetector.CONFIG_OF_MAX_RETRIES, Integer.MAX_VALUE);

    _failureDetector = FailureDetectorFactory.getFailureDetector(config, BrokerMetrics.get());
    _excludedServers = new CopyOnWriteArrayList<>();
    _reincludedServers = new CopyOnWriteArrayList<>();
    _failureDetector.registerUnhealthyServerNotifier(instanceId -> _excludedServers.add(instanceId));
    _failureDetector.registerHealthyServerNotifier(instanceId -> _reincludedServers.add(instanceId));
    _failureDetector.start();

    BrokerQueryEventListenerFactory.init(new PinotConfiguration());
    ServerRoutingStatsManager serverRoutingStatsManager =
        new ServerRoutingStatsManager(new PinotConfiguration(), BrokerMetrics.get());
    serverRoutingStatsManager.init();

    _routingManager = mock(BrokerRoutingManager.class);
    when(_routingManager.getEnabledServerInstanceMap()).thenReturn(_enabledServers);
    _requestHandler =
        new SingleConnectionBrokerRequestHandler(config, "testBroker", new BrokerRequestIdGenerator(), _routingManager,
            new AllowAllAccessControlFactory(), mock(QueryQuotaManager.class), mock(TableCache.class), null, null,
            serverRoutingStatsManager, _failureDetector, ThreadAccountantUtils.getNoOpAccountant(),
            mock(MultiClusterRoutingContext.class));
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    stopBroker();
    for (QueryServer queryServer : _queryServers) {
      queryServer.shutDown();
    }
    _queryServers.clear();
    for (ServerSocket frozenServer : _frozenServers) {
      frozenServer.close();
    }
    _frozenServers.clear();
    _enabledServers.clear();
  }

  private void stopBroker() {
    if (_requestHandler != null) {
      _requestHandler.shutDown();
      _requestHandler = null;
    }
    if (_failureDetector != null) {
      _failureDetector.stop();
      _failureDetector = null;
    }
    // The gauge is a JVM-wide singleton; do not leave it dirty for the next test.
    BrokerMetrics.get().setValueOfGlobalGauge(BrokerGauge.UNHEALTHY_SERVERS, 0);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------------------------------------------

  /// Starts a server whose node is gone or whose JVM is frozen: a listening socket that is never accepted from. The
  /// kernel completes the TCP handshake and buffers whatever is sent, but nothing ever reads it or replies -- neither
  /// queries nor pings. Shutting a server down would NOT model this: that closes the socket, which takes the
  /// already-working connection-failure path instead.
  private ServerInstance startFrozenServer()
      throws IOException {
    ServerSocket frozenServer = new ServerSocket(0, 100);
    _frozenServers.add(frozenServer);
    return enable(QueryServerTestUtils.serverInstance("localhost", frozenServer.getLocalPort()));
  }

  /// Starts a server whose JVM is alive but whose query engine never finishes anything -- stuck, or so busy that every
  /// query times out. Its network thread still answers pings, because they do not go through the query engine.
  private ServerInstance startStuckQueryEngineServer() {
    return startQueryServer(0, invocation -> SettableFuture.<byte[]>create());
  }

  /// Starts a server that answers queries straight away.
  private ServerInstance startRespondingServer(int port) {
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(REQUEST_ID));
    byte[] responseBytes;
    try {
      responseBytes = dataTable.toBytes();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return startQueryServer(port, invocation -> Futures.immediateFuture(responseBytes));
  }

  private ServerInstance startQueryServer(int port, Answer<?> submitAnswer) {
    QueryServer queryServer = QueryServerTestUtils.newQueryServer(port, submitAnswer);
    _queryServers.add(queryServer);
    return enable(QueryServerTestUtils.serverInstance("localhost", QueryServerTestUtils.startAndGetPort(queryServer)));
  }

  private ServerInstance enable(ServerInstance serverInstance) {
    _enabledServers.put(serverInstance.getInstanceId(), serverInstance);
    return serverInstance;
  }

  /// Routes a query to the given servers for the given table types; both for a hybrid table.
  private static TableRouteInfo routeTo(boolean offline, boolean realtime, ServerInstance... servers) {
    Map<ServerInstance, SegmentsToQuery> routingTable = new HashMap<>();
    for (ServerInstance server : servers) {
      routingTable.put(server, new SegmentsToQuery(List.of("segment0"), List.of()));
    }
    return new ImplicitHybridTableRouteInfo(offline ? BROKER_REQUEST : null, realtime ? BROKER_REQUEST : null,
        offline ? routingTable : null, realtime ? routingTable : null);
  }

  private static TableRouteInfo offlineRouteTo(ServerInstance... servers) {
    return routeTo(true, false, servers);
  }

  private SingleConnectionBrokerRequestHandler.ScatterResult scatter(TableRouteInfo route)
      throws Exception {
    return _requestHandler.doScatter(REQUEST_ID, RAW_TABLE_NAME, route, QUERY_TIMEOUT_MS,
        new BaseSingleStageBrokerRequestHandler.ServerStats());
  }

  /// Waits until exactly `servers` are unhealthy, gauge included. Waits on that final state rather than on the
  /// notifier, which runs before the detector records the change.
  private void awaitUnhealthyServers(ServerInstance... servers) {
    Set<String> expected = new HashSet<>();
    for (ServerInstance server : servers) {
      expected.add(server.getInstanceId());
    }
    TestUtils.waitForCondition(
        aVoid -> _failureDetector.getUnhealthyServers().equals(expected) && unhealthyServerGauge() == expected.size(),
        10_000L, "Unhealthy servers did not become: " + expected);
  }

  private static int unhealthyServerGauge() {
    return (int) MetricValueUtils.getGlobalGaugeValue(BrokerMetrics.get(), BrokerGauge.UNHEALTHY_SERVERS);
  }

  /// Waits until the check of `server` after a timeout has finished, not merely started, given the count of its
  /// lookups. A server is checked again only once its previous check is over, so a second check proves the first
  /// finished. Stops early if the server got marked unhealthy, which ends its checks, and leaves that to the caller's
  /// assertions.
  private void awaitCheckFinished(ServerInstance server, AtomicInteger numLookups) {
    TestUtils.waitForCondition(aVoid -> {
      _failureDetector.notifyServerNotResponded(server.getInstanceId(), server.getHostname());
      return numLookups.get() >= 2 || !_failureDetector.getUnhealthyServers().isEmpty();
    }, 10_000L, "Check did not finish");
  }

  /// Counts lookups of enabled servers. The handler looks a server up once per check or retry, so this counts those.
  private AtomicInteger countServerLookups() {
    AtomicInteger numLookups = new AtomicInteger();
    when(_routingManager.getEnabledServerInstanceMap()).thenAnswer(invocation -> {
      numLookups.incrementAndGet();
      return _enabledServers;
    });
    return numLookups;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------------------------------------------

  /// THE INCIDENT. A query to a server whose node is gone times out; the broker pings the server, the ping goes
  /// unanswered too, and the server is taken out of routing -- about one query timeout plus one ping timeout after the
  /// first failure, instead of minutes later.
  ///
  /// Covered for every table type: the broker keeps a separate channel per table type, and the incident's tables were
  /// REALTIME. Checking only the OFFLINE channel would never have pinged this server at all.
  @Test(dataProvider = "tableTypes")
  public void testServerWhoseNodeIsGoneIsTakenOutOfRouting(boolean offline, boolean realtime)
      throws Exception {
    ServerInstance frozenServer = startFrozenServer();

    long startTimeMs = System.currentTimeMillis();
    SingleConnectionBrokerRequestHandler.ScatterResult result = scatter(routeTo(offline, realtime, frozenServer));
    assertTrue(result.isTimedOut());
    awaitUnhealthyServers(frozenServer);
    long elapsedMs = System.currentTimeMillis() - startTimeMs;

    assertEquals(_excludedServers, List.of(frozenServer.getInstanceId()));
    assertTrue(elapsedMs < QUERY_TIMEOUT_MS + PING_TIMEOUT_MS + 3_000L,
        "Server should be out of routing about one query timeout plus one ping timeout in; took " + elapsedMs + "ms");
  }

  @DataProvider(name = "tableTypes")
  public Object[][] tableTypes() {
    return new Object[][]{{true, false}, {false, true}, {true, true}};
  }

  /// A server whose JVM is alive but whose queries time out -- stuck or merely busy -- answers the ping and stays in
  /// routing. This is the control for the test above: the same timeout and the same check, but here the ping is
  /// answered. It also pins the accepted gap: a live JVM with a stuck query engine is not detected.
  ///
  /// A retrier that calls every server unhealthy is registered too, as the multi-stage one does for an idle gRPC
  /// channel: only the ping may decide whether a server that left a query unanswered leaves routing.
  @Test
  public void testServerWhoseQueryEngineIsStuckStaysInRouting()
      throws Exception {
    ServerInstance stuckServer = startStuckQueryEngineServer();
    _failureDetector.registerUnhealthyServerRetrier(instanceId -> FailureDetector.ServerState.UNHEALTHY);
    AtomicInteger numChecks = countServerLookups();

    assertTrue(scatter(offlineRouteTo(stuckServer)).isTimedOut());
    awaitCheckFinished(stuckServer, numChecks);

    assertEquals(_failureDetector.getUnhealthyServers(), Set.of());
    assertEquals(_excludedServers, List.of());
  }

  /// A server outside this broker's routing -- another cluster's, say, or one Helix has just disabled -- is left
  /// alone after a timeout: there is nothing to take out of routing.
  @Test
  public void testServerOutsideThisBrokersRoutingIsLeftAlone()
      throws Exception {
    ServerInstance frozenServer = startFrozenServer();
    _enabledServers.remove(frozenServer.getInstanceId());
    AtomicInteger numChecks = countServerLookups();

    assertTrue(scatter(offlineRouteTo(frozenServer)).isTimedOut());
    awaitCheckFinished(frozenServer, numChecks);

    assertEquals(_failureDetector.getUnhealthyServers(), Set.of());
    assertEquals(_excludedServers, List.of());
  }

  /// A server the broker has no query channel to has nothing to ping over, which says nothing about the server, so it
  /// is left alone too.
  @Test
  public void testServerWithoutAQueryChannelIsLeftAlone() {
    ServerInstance respondingServer = startRespondingServer(0);
    AtomicInteger numChecks = countServerLookups();

    awaitCheckFinished(respondingServer, numChecks);

    assertEquals(_failureDetector.getUnhealthyServers(), Set.of());
    assertEquals(_excludedServers, List.of());
  }

  /// Pings are off by default, which leaves a timeout without consequence, as before this change. Old servers queue a
  /// ping behind their queries, so a busy one could miss it; the feature is for clusters whose servers all answer
  /// pings.
  @Test
  public void testTimeoutLeavesServerInRoutingWhenPingsAreOff()
      throws Exception {
    stopBroker();
    startBroker(false, RETRIES_OFF_MS);
    ServerInstance frozenServer = startFrozenServer();
    AtomicInteger numLookups = countServerLookups();

    assertTrue(scatter(offlineRouteTo(frozenServer)).isTimedOut());
    // A check would look the server up within milliseconds; give one far longer than that before concluding none ran.
    Thread.sleep(PING_TIMEOUT_MS + 500L);

    assertEquals(numLookups.get(), 0);
    assertEquals(_failureDetector.getUnhealthyServers(), Set.of());
    assertEquals(_excludedServers, List.of());
  }

  /// With pings off, retries connect to the server, as before this change, rather than ping it. A connect succeeds
  /// even against a frozen server, whose kernel still completes the handshake, so this one is put back at once.
  @Test
  public void testRetriesConnectWhenPingsAreOff()
      throws Exception {
    stopBroker();
    startBroker(false, FAST_RETRY_DELAY_MS);
    ServerInstance frozenServer = startFrozenServer();
    // Opens the channel a retry looks for; without one the retrier has no opinion.
    assertTrue(scatter(offlineRouteTo(frozenServer)).isTimedOut());

    _failureDetector.markServerUnhealthy(frozenServer.getInstanceId(), frozenServer.getHostname());
    awaitUnhealthyServers();
    assertEquals(_reincludedServers, List.of(frozenServer.getInstanceId()));
  }

  /// Several servers dying together are all taken out of routing, while their healthy peer is left alone.
  @Test
  public void testEveryFrozenServerIsTakenOutOfRouting()
      throws Exception {
    ServerInstance frozenServer1 = startFrozenServer();
    ServerInstance frozenServer2 = startFrozenServer();
    ServerInstance respondingServer = startRespondingServer(0);

    SingleConnectionBrokerRequestHandler.ScatterResult result =
        scatter(offlineRouteTo(frozenServer1, frozenServer2, respondingServer));
    assertTrue(result.isTimedOut());
    assertEquals(result.getServersNotResponded().size(), 2);
    awaitUnhealthyServers(frozenServer1, frozenServer2);
  }

  /// A server that keeps failing its pings stays out of routing, retry after retry. Each retry reconnects -- the
  /// previous channel was closed when its ping failed -- and pings over the new channel.
  @Test
  public void testFrozenServerStaysOutOfRoutingWhileItKeepsFailingPings()
      throws Exception {
    stopBroker();
    startBroker(true, FAST_RETRY_DELAY_MS);
    ServerInstance frozenServer = startFrozenServer();
    AtomicInteger numChecks = countServerLookups();

    scatter(offlineRouteTo(frozenServer));
    awaitUnhealthyServers(frozenServer);
    // The first check marked it unhealthy; wait for retries on top of it, so this cannot pass vacuously.
    TestUtils.waitForCondition(aVoid -> numChecks.get() >= 4, 10_000L, "Server was not retried");

    assertEquals(_failureDetector.getUnhealthyServers(), Set.of(frozenServer.getInstanceId()));
    assertEquals(_reincludedServers, List.of());
  }

  /// A server that comes back -- after a long GC pause, or a restart in place -- is put back into routing by the
  /// retrier once it answers a ping again. Its old channel was closed when the first ping failed, so the retry has to
  /// reconnect to reach it.
  @Test
  public void testServerIsPutBackOnceItAnswersAgain()
      throws Exception {
    stopBroker();
    startBroker(true, FAST_RETRY_DELAY_MS);
    ServerInstance frozenServer = startFrozenServer();
    int port = frozenServer.getPort();

    scatter(offlineRouteTo(frozenServer));
    awaitUnhealthyServers(frozenServer);

    // The server recovers on the same port.
    _frozenServers.remove(0).close();
    startRespondingServer(port);

    awaitUnhealthyServers();
    assertEquals(_reincludedServers, List.of(frozenServer.getInstanceId()));
  }
}

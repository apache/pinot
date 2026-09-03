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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Reproduction and regression coverage for the "broker keeps routing to a wedged server" defect.
///
/// A Pinot server that stops answering queries but keeps its TCP connection open — a wedged process, a blackholed
/// network path, a disk-stalled JVM — produces query *timeouts* and nothing else. It never raises a send exception
/// and never tears the Netty channel down, so
/// [org.apache.pinot.core.transport.AsyncQueryResponse#getFailedServer()] stays `null`. Before the fix that was the
/// only signal [SingleConnectionBrokerRequestHandler#doScatter] fed to the [FailureDetector], so the broker kept
/// selecting the dead server for every subsequent query while healthy replicas sat idle, until Helix eventually
/// noticed minutes later.
///
/// These tests drive the real [SingleConnectionBrokerRequestHandler] and a real [FailureDetector] against real
/// Netty query servers, so the whole broker-side chain (timeout → failure detector → routing exclusion callback)
/// is exercised over real sockets rather than mocks.
public class SingleConnectionBrokerRequestHandlerFailureDetectorTest {
  private static final BrokerRequest BROKER_REQUEST =
      CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final long REQUEST_ID = 123L;
  private static final long TIMEOUT_MS = 1_000L;
  private static final int TIMEOUT_THRESHOLD = 3;
  /// Long enough that the retry thread never fires underneath a test that is not about recovery.
  private static final long RETRY_DISABLED_DELAY_MS = 600_000L;
  private static final long FAST_RETRY_DELAY_MS = 100L;

  private final List<QueryServer> _startedServers = new ArrayList<>();

  private BrokerRoutingManager _routingManager;
  private SingleConnectionBrokerRequestHandler _requestHandler;
  private FailureDetector _failureDetector;
  private List<String> _excludedServers;
  private List<String> _reincludedServers;

  @BeforeMethod
  public void setUp() {
    startBroker(RETRY_DISABLED_DELAY_MS);
  }

  /// Builds the broker-side stack under test: a real request handler wired to a real failure detector whose
  /// notifiers are recorded, mirroring the wiring `BaseBrokerStarter` performs in production.
  private void startBroker(long retryInitialDelayMs) {
    // BrokerMetrics is a JVM-wide singleton, so the gauge carries over from whatever ran before in this fork.
    BrokerMetrics.get().setValueOfGlobalGauge(BrokerGauge.UNHEALTHY_SERVERS, 0);

    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Broker.FailureDetector.CONFIG_OF_TYPE, Broker.FailureDetector.Type.CONNECTION.name());
    config.setProperty(Broker.FailureDetector.CONFIG_OF_CONSECUTIVE_TIMEOUT_THRESHOLD, TIMEOUT_THRESHOLD);
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_INITIAL_DELAY_MS, retryInitialDelayMs);

    _failureDetector = FailureDetectorFactory.getFailureDetector(config, BrokerMetrics.get());
    // Mirror BaseBrokerStarter: an unhealthy server is excluded from routing, a recovered one is put back.
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
    _requestHandler =
        new SingleConnectionBrokerRequestHandler(config, "testBroker", new BrokerRequestIdGenerator(), _routingManager,
            new AllowAllAccessControlFactory(), mock(QueryQuotaManager.class), mock(TableCache.class), null, null,
            serverRoutingStatsManager, _failureDetector, ThreadAccountantUtils.getNoOpAccountant(),
            mock(MultiClusterRoutingContext.class));
  }

  @AfterMethod
  public void tearDown() {
    stopBroker();
    stopStartedServers();
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

  private void stopStartedServers() {
    for (QueryServer server : _startedServers) {
      server.shutDown();
    }
    _startedServers.clear();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------------------------------------------

  /// Starts a server that accepts connections and requests but never writes a response.
  ///
  /// A [SettableFuture] that is never completed is a faithful stand-in for a wedged server: the event loop stays
  /// free, so the connection stays healthy from the broker's point of view, and the response callback simply never
  /// fires. It must NOT be simulated by shutting the server down -- that tears the channel down and exercises the
  /// already-working `markServerDown` path instead of the defect under test.
  private ServerInstance startWedgedServer() {
    return startServer(invocation -> SettableFuture.<byte[]>create());
  }

  /// Starts a server that answers immediately with an empty DataTable carrying [#REQUEST_ID], which is the request
  /// id every scatter in this class uses. A response whose request id does not match is dropped by
  /// `DataTableHandler`, which would silently turn this into a second wedged server.
  private ServerInstance startRespondingServer()
      throws Exception {
    byte[] responseBytes = emptyDataTableBytes();
    return startServer(invocation -> Futures.immediateFuture(responseBytes));
  }

  /// Starts a server that answers every other request, so a hybrid query gets exactly one of its two halves back.
  /// Both halves arrive on the same channel and are decoded on the same event loop, so the alternation is stable.
  private ServerInstance startHalfAnsweringServer()
      throws Exception {
    byte[] responseBytes = emptyDataTableBytes();
    AtomicInteger numRequests = new AtomicInteger();
    return startServer(invocation -> numRequests.getAndIncrement() % 2 == 0 ? Futures.immediateFuture(responseBytes)
        : SettableFuture.<byte[]>create());
  }

  private ServerInstance startServer(Answer<?> submitAnswer) {
    QueryServer server = QueryServerTestUtils.newQueryServer(submitAnswer);
    _startedServers.add(server);
    return QueryServerTestUtils.serverInstance("localhost", QueryServerTestUtils.startAndGetPort(server));
  }

  private static byte[] emptyDataTableBytes()
      throws Exception {
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(REQUEST_ID));
    return dataTable.toBytes();
  }

  private static TableRouteInfo routeTo(ServerInstance... servers) {
    Map<ServerInstance, SegmentsToQuery> routingTable = new HashMap<>();
    for (ServerInstance server : servers) {
      routingTable.put(server, new SegmentsToQuery(List.of("segment0"), List.of()));
    }
    return new ImplicitHybridTableRouteInfo(BROKER_REQUEST, null, routingTable, null);
  }

  private static TableRouteInfo hybridRouteTo(ServerInstance server) {
    Map<ServerInstance, SegmentsToQuery> routingTable =
        Map.of(server, new SegmentsToQuery(List.of("segment0"), List.of()));
    return new ImplicitHybridTableRouteInfo(BROKER_REQUEST, BROKER_REQUEST, routingTable, routingTable);
  }

  private SingleConnectionBrokerRequestHandler.ScatterResult scatter(TableRouteInfo route)
      throws Exception {
    return _requestHandler.doScatter(REQUEST_ID, RAW_TABLE_NAME, route, TIMEOUT_MS,
        new BaseSingleStageBrokerRequestHandler.ServerStats());
  }

  /// Runs one scatter and asserts it timed out with every routed server reported as not-responded, which is what
  /// the customer-visible `427 SERVER_NOT_RESPONDING` error is built from.
  private void scatterAndAssertAllTimedOut(TableRouteInfo route, int expectedNumServers)
      throws Exception {
    SingleConnectionBrokerRequestHandler.ScatterResult result = scatter(route);
    assertTrue(result.isTimedOut(), "Query against a wedged server must time out");
    assertEquals(result.getDataTableMap().size(), 0, "Wedged server must not have returned a DataTable");
    assertEquals(result.getServersNotResponded().size(), expectedNumServers);
    // Nothing broke at the transport layer, so getFailedServer() was null and connection-failure detection --
    // the only mechanism that existed before the fix -- had nothing to report.
    assertNull(result.getSendException(), "A wedged server produces no send exception");
  }

  private static int unhealthyServerGauge() {
    return (int) MetricValueUtils.getGlobalGaugeValue(BrokerMetrics.get(), BrokerGauge.UNHEALTHY_SERVERS);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------------------------------------------

  /// THE REPRODUCTION.
  ///
  /// Before the fix this test failed on the last assertions: after repeated timeouts against the same wedged
  /// server the failure detector had never been told about it, so the server was never excluded from routing and
  /// every subsequent query was scattered to it again.
  @Test
  public void testRepeatedTimeoutsAgainstWedgedServerMarkItUnhealthy()
      throws Exception {
    ServerInstance wedgedServer = startWedgedServer();
    TableRouteInfo route = routeTo(wedgedServer);

    for (int i = 0; i < TIMEOUT_THRESHOLD; i++) {
      scatterAndAssertAllTimedOut(route, 1);
      if (i < TIMEOUT_THRESHOLD - 1) {
        // A slow-but-healthy server, one heavy query or a GC pause must never cost a replica; only sustained
        // silence may.
        assertEquals(_failureDetector.getUnhealthyServers(), Set.of(),
            "A server must not be ejected before it has missed the configured number of consecutive requests");
      }
    }

    assertEquals(_failureDetector.getUnhealthyServers(), Set.of(wedgedServer.getInstanceId()),
        "A server that repeatedly leaves requests unanswered while its connection stays open must be marked "
            + "unhealthy. If this set is empty the broker keeps routing queries to the dead server until Helix "
            + "notices minutes later, while healthy replicas sit idle.");
    assertEquals(_excludedServers, List.of(wedgedServer.getInstanceId()),
        "The unhealthy-server notifier must fire exactly once so the server is excluded from routing");
    assertEquals(unhealthyServerGauge(), 1);
  }

  /// Safety rule: a timeout is only attributed to a server that is the sole non-responder. When several servers miss
  /// the same deadline a shared cause — the broker, the query, or the network — is far more likely than every server
  /// failing at once, and ejecting them all would turn a slow query into a hard outage.
  @Test(dataProvider = "numRespondingPeers")
  public void testTimeoutSharedBySeveralServersEjectsNobody(int numRespondingPeers)
      throws Exception {
    List<ServerInstance> servers = new ArrayList<>(List.of(startWedgedServer(), startWedgedServer()));
    for (int i = 0; i < numRespondingPeers; i++) {
      servers.add(startRespondingServer());
    }
    TableRouteInfo route = routeTo(servers.toArray(new ServerInstance[0]));

    // Well past the threshold: the rule is unconditional, not merely a delay.
    for (int i = 0; i < TIMEOUT_THRESHOLD + 2; i++) {
      SingleConnectionBrokerRequestHandler.ScatterResult result = scatter(route);
      assertTrue(result.isTimedOut());
      assertEquals(result.getServersNotResponded().size(), 2);
    }

    assertEquals(_failureDetector.getUnhealthyServers(), Set.of(),
        "No server may be ejected when more than one of them failed to respond");
    assertEquals(_excludedServers, List.of());
    assertEquals(unhealthyServerGauge(), 0);
  }

  /// Covers both shapes of a shared timeout: the whole route failing, and part of it failing while peers answer.
  @DataProvider(name = "numRespondingPeers")
  public Object[][] numRespondingPeers() {
    return new Object[][]{{0}, {1}};
  }

  /// The partial-failure case that matters in production: one server in the route is wedged while its peers keep
  /// answering. Only the wedged server is ejected, and the healthy peer is never touched.
  @Test
  public void testOnlyTheNonRespondingServerInAPartiallyHealthyRouteIsEjected()
      throws Exception {
    ServerInstance wedgedServer = startWedgedServer();
    ServerInstance healthyServer = startRespondingServer();
    TableRouteInfo route = routeTo(wedgedServer, healthyServer);

    for (int i = 0; i < TIMEOUT_THRESHOLD; i++) {
      SingleConnectionBrokerRequestHandler.ScatterResult result = scatter(route);
      assertTrue(result.isTimedOut());
      assertEquals(result.getDataTableMap().size(), 1, "The healthy server must have answered");
      assertEquals(result.getServersNotResponded().size(), 1);
      assertEquals(result.getServersNotResponded().get(0).getInstanceId(), wedgedServer.getInstanceId());
    }

    assertEquals(_failureDetector.getUnhealthyServers(), Set.of(wedgedServer.getInstanceId()),
        "Only the server that stopped answering may be ejected");
    assertEquals(_excludedServers, List.of(wedgedServer.getInstanceId()));
  }

  /// A hybrid query sends a separate request to the OFFLINE and REALTIME halves of the same server, which shows up
  /// as two routing instances sharing one instance id. The sole-non-responder rule must count those as one server, or
  /// a hybrid table served by a single wedged server would be shielded from detection forever.
  @Test
  public void testHybridRouteToOneWedgedServerCountsAsOneNonResponder()
      throws Exception {
    ServerInstance wedgedServer = startWedgedServer();
    TableRouteInfo hybridRoute = hybridRouteTo(wedgedServer);

    // Each scatter leaves two requests -- the OFFLINE one and the REALTIME one -- unanswered on the one server.
    for (int i = 0; i < TIMEOUT_THRESHOLD; i++) {
      scatterAndAssertAllTimedOut(hybridRoute, 2);
    }

    assertEquals(_failureDetector.getUnhealthyServers(), Set.of(wedgedServer.getInstanceId()),
        "Two routing instances for the same physical server must count as one non-responder, or the sole-non-"
            + "responder rule would permanently shield a single-server hybrid table from detection");
  }

  /// A hybrid server that answers one half of the query and stalls the other is deliberately never ejected: the
  /// answered half resets the count that the stalled half then sets back to one, so the threshold is never reached.
  ///
  /// This is the conservative direction and it is intentional. A server producing results for some requests is
  /// serving, and pulling it costs capacity that the remaining requests still need. It is pinned here because it
  /// falls out of the ordering inside `reportScatterOutcomeToFailureDetector` rather than from an explicit branch.
  @Test
  public void testHybridServerAnsweringOneHalfIsNeverEjected()
      throws Exception {
    ServerInstance server = startHalfAnsweringServer();
    TableRouteInfo hybridRoute = hybridRouteTo(server);

    for (int i = 0; i < TIMEOUT_THRESHOLD + 2; i++) {
      SingleConnectionBrokerRequestHandler.ScatterResult result = scatter(hybridRoute);
      assertEquals(result.getDataTableMap().size(), 1, "The OFFLINE half must have answered");
      assertEquals(result.getServersNotResponded().size(), 1, "The REALTIME half must not have answered");
    }

    assertEquals(_failureDetector.getUnhealthyServers(), Set.of(),
        "A server that answers part of a hybrid query is still serving and must not be ejected");
  }

  /// A server that still accepts connections is put back into routing by the failure detector's existing retrier.
  ///
  /// This is the deliberate fail-safe direction, and it pins the known limit of the probe: it proves the server can
  /// still be reached, not that it can still answer a query. A process that is alive but wedged passes it. Erring
  /// this way keeps a merely busy server from being stranded, which is the outcome that would make an overload worse.
  @Test
  public void testServerThatAcceptsConnectionsIsIncludedAgain()
      throws Exception {
    stopBroker();
    startBroker(FAST_RETRY_DELAY_MS);
    ServerInstance wedgedServer = startWedgedServer();
    when(_routingManager.getEnabledServerInstanceMap()).thenReturn(
        Map.of(wedgedServer.getInstanceId(), wedgedServer));
    TableRouteInfo route = routeTo(wedgedServer);
    for (int i = 0; i < TIMEOUT_THRESHOLD; i++) {
      scatterAndAssertAllTimedOut(route, 1);
    }
    assertEquals(_excludedServers, List.of(wedgedServer.getInstanceId()));

    TestUtils.waitForCondition(aVoid -> _reincludedServers.contains(wedgedServer.getInstanceId()), 10_000L,
        "Failed to re-include the server through the failure detector's retrier");
    assertEquals(_failureDetector.getUnhealthyServers(), Set.of());
    assertEquals(unhealthyServerGauge(), 0);
  }

  /// A server that has stopped accepting connections stays ejected, and the retry backoff grows.
  ///
  /// This is what the fresh-connection probe buys. The pooled channel to a server whose node disappeared stays
  /// established — nothing sends a FIN or an RST — so the previous probe reused it, always succeeded, and put the
  /// dead server back into routing every few seconds.
  @Test
  public void testServerThatRefusesConnectionsStaysEjected()
      throws Exception {
    stopBroker();
    startBroker(FAST_RETRY_DELAY_MS);
    ServerInstance wedgedServer = startWedgedServer();
    // retryUnhealthyServer() looks the server up here once per probe, so this counts probes.
    AtomicInteger numProbes = new AtomicInteger();
    when(_routingManager.getEnabledServerInstanceMap()).thenAnswer(invocation -> {
      numProbes.incrementAndGet();
      return Map.of(wedgedServer.getInstanceId(), wedgedServer);
    });

    // One scatter, so that the query router holds a pooled channel to the server -- without one the retrier bails
    // out with UNKNOWN before it ever probes. This stays below the ejection threshold.
    scatterAndAssertAllTimedOut(routeTo(wedgedServer), 1);
    assertEquals(_failureDetector.getUnhealthyServers(), Set.of());

    // Stop the server BEFORE anything is queued for retry, so the retry loop can never race a live listener. The
    // pooled channel is left behind exactly as it would be by a server whose node vanished.
    stopStartedServers();
    _failureDetector.markServerUnhealthy(wedgedServer.getInstanceId(), wedgedServer.getHostname());
    assertEquals(_excludedServers, List.of(wedgedServer.getInstanceId()));

    // Wait until the retrier has actually probed several times, so the assertion below cannot pass vacuously.
    TestUtils.waitForCondition(aVoid -> numProbes.get() >= 3, 10_000L, "Retrier did not probe the server");
    assertEquals(_failureDetector.getUnhealthyServers(), Set.of(wedgedServer.getInstanceId()));
    assertEquals(_reincludedServers, List.of());
  }
}

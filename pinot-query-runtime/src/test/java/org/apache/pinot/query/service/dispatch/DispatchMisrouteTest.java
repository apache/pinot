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
package org.apache.pinot.query.service.dispatch;

import io.grpc.ConnectivityState;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.grpc.ReceiverIdentity;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.QueryPlanSerDeUtils;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.service.dispatch.streaming.StreamingQuerySession;
import org.apache.pinot.query.service.server.QueryServer;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.query.testutils.TcpRelay;
import org.apache.pinot.query.testutils.TestDnsNameResolverProvider;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request.MetadataKeys;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// A broker dispatching a stage plan to the wrong server after the target's address is handed to another server that
/// listens on the same pinned query server port. See `MailboxMisdeliveryTest` for the mailbox side.
public class DispatchMisrouteTest {
  private static final String SERVER_A = "server-a.pinot-test";
  private static final String SERVER_A_ID = "Server_" + SERVER_A + "_8098";
  private static final String SERVER_B_ID = "Server_server-b.pinot-test_8098";
  private static final int PINNED_QUERY_SERVER_PORT = 8100;
  private static final int PINNED_MAILBOX_PORT = 8101;
  private static final QueryServerInstance TARGET_A =
      new QueryServerInstance(SERVER_A_ID, SERVER_A, PINNED_QUERY_SERVER_PORT, PINNED_MAILBOX_PORT);

  private final List<QueryServer> _queryServers = new ArrayList<>();
  private TestDnsNameResolverProvider _dns;
  private TcpRelay _addressX;
  private FailureDetector _failureDetector;
  private QueryDispatcher _queryDispatcher;

  @BeforeMethod
  public void setUp() {
    ServerMetrics.deregister();
    ServerMetrics.register(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    BrokerMetrics.register(new BrokerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    _dns = TestDnsNameResolverProvider.register();
    _failureDetector = mock(FailureDetector.class);
    _queryDispatcher =
        new QueryDispatcher(mock(MailboxService.class), _failureDetector, null, false, Duration.ofSeconds(1));
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    try {
      _queryDispatcher.shutdown();
      for (QueryServer queryServer : _queryServers) {
        queryServer.shutdown();
      }
      _queryServers.clear();
      if (_addressX != null) {
        _addressX.close();
        _addressX = null;
      }
    } finally {
      _dns.close();
      ServerMetrics.deregister();
    }
  }

  /// The incident, with servers that do not check the target of a request (older versions, or the check disabled):
  /// the broker's channel to A still ends at A's old IP, which B now owns, and B runs the plan meant for A.
  @Test
  public void testStaleChannelMisdispatchesWithoutCheck()
      throws Exception {
    Swap swap = swapAddresses(false);

    AsyncResponse<Worker.QueryResponse> response = submit(TARGET_A);
    // B runs the plan meant for A and reports success
    assertNull(response.getThrowable());
    assertNotNull(response.getResponse());
    verify(swap._serverB, timeout(10_000L)).submit(any(), any());
    verify(swap._restartedServerA, never()).submit(any(), any());
  }

  /// B rejects the plan meant for A before running it, the broker resets its channel, and the next plan reaches A.
  @Test
  public void testMisroutedDispatchIsRejected()
      throws Exception {
    Swap swap = swapAddresses(true);
    long rejectedBefore = ServerMetrics.get().getMeteredValue(ServerMeter.MULTI_STAGE_MISROUTED_REQUESTS_REJECTED)
        .count();
    long rejectedByPeerBefore =
        BrokerMetrics.get().getMeteredValue(BrokerMeter.MULTI_STAGE_MISROUTED_REQUESTS_REJECTED_BY_PEER).count();

    AsyncResponse<Worker.QueryResponse> response = submit(TARGET_A);
    assertNull(response.getResponse());
    assertEquals(Status.fromThrowable(response.getThrowable()).getCode(), Status.Code.FAILED_PRECONDITION);
    String misroutedTo = ReceiverIdentity.getMisroutedTo(response.getThrowable());
    assertNotNull(misroutedTo);
    assertTrue(misroutedTo.contains(SERVER_B_ID), misroutedTo);
    verify(swap._serverB, never()).submit(any(), any());
    assertEquals(ServerMetrics.get().getMeteredValue(ServerMeter.MULTI_STAGE_MISROUTED_REQUESTS_REJECTED).count(),
        rejectedBefore + 1);
    assertEquals(
        BrokerMetrics.get().getMeteredValue(BrokerMeter.MULTI_STAGE_MISROUTED_REQUESTS_REJECTED_BY_PEER).count(),
        rejectedByPeerBefore + 1);

    // The dispatcher fails the query with a clear error, and does not blame the server
    QueryException e = expectThrows(QueryException.class, () -> processResults(response));
    assertEquals(e.getErrorCode(), QueryErrorCode.SERVER_NOT_RESPONDING);
    assertTrue(e.getMessage().contains(SERVER_B_ID) && e.getMessage().contains("stale"), e.getMessage());
    verify(_failureDetector, never()).markServerUnhealthy(anyString(), anyString());

    // The channel resolved the hostname again: the next plan reaches A
    assertNull(submit(TARGET_A).getThrowable());
    verify(swap._restartedServerA, timeout(10_000L)).submit(any(), any());
    verify(swap._serverB, never()).submit(any(), any());
  }

  /// The other kinds of requests to a server are checked too, and their rejection resets the channel as well.
  @Test(dataProvider = "otherRequestKinds")
  public void testMisroutedRequestOfOtherKindIsRejected(String requestKind)
      throws Exception {
    Swap swap = swapAddresses(true);

    Throwable t = sendRequest(requestKind);
    assertTrue(ReceiverIdentity.isMisrouted(t), String.valueOf(t));
    verify(swap._serverB, never()).cancel(any(), any());
    verify(swap._serverB, never()).explain(any(), any());
    verify(swap._serverB, never()).submitWithStream(any());

    // The channel resolved the hostname again: the next plan reaches A
    assertNull(submit(TARGET_A).getThrowable());
    verify(swap._restartedServerA, timeout(10_000L)).submit(any(), any());
  }

  @DataProvider
  public Object[][] otherRequestKinds() {
    return new Object[][]{{"cancel"}, {"explain"}, {"submitWithStream"}};
  }

  /// A request that does not say which instance it is for, as from an older broker, is accepted.
  @Test
  public void testRequestWithoutTargetIsAccepted()
      throws Exception {
    int port = QueryTestUtils.getAvailablePort();
    _dns.map(SERVER_A, port);
    QueryServer serverA = startQueryServer(SERVER_A_ID, port, true);
    ManagedChannel channel = _queryDispatcher.getOrCreateDispatchClient(TARGET_A).getChannel();

    CompletableFuture<AsyncResponse<Worker.QueryResponse>> future = new CompletableFuture<>();
    PinotQueryWorkerGrpc.newStub(channel).withDeadline(Deadline.after(10, TimeUnit.SECONDS))
        .submit(request(), new LastValueDispatchObserver<>(TARGET_A, future::complete));
    assertNull(future.get(10, TimeUnit.SECONDS).getThrowable());
    verify(serverA).submit(any(), any());
  }

  /// Puts the test in the state of the incident: the broker's channel to A points to A's old address X, which server
  /// B has taken over, while A runs again at a new address that DNS now returns.
  private Swap swapAddresses(boolean rejectMisrouted)
      throws Exception {
    int portA = QueryTestUtils.getAvailablePort();
    QueryServer serverA = startQueryServer(SERVER_A_ID, portA, rejectMisrouted);
    _addressX = new TcpRelay(portA);
    _dns.map(SERVER_A, _addressX.getPort());
    assertNull(submit(TARGET_A).getThrowable());
    verify(serverA, times(1)).submit(any(), any());

    // A's pod goes away; gRPC resolves A's hostname right then, while DNS still returns X
    ManagedChannel channel = _queryDispatcher.getOrCreateDispatchClient(TARGET_A).getChannel();
    int numResolutions = _dns.getNumResolutions();
    stopQueryServer(serverA);
    TestUtils.waitForCondition(
        aVoid -> channel.getState(false) != ConnectivityState.READY && _dns.getNumResolutions() > numResolutions,
        10_000L, "The channel did not notice that server A went away");

    // B takes over address X, on the same pinned port. A comes back at another address, which DNS returns from now on.
    int portB = QueryTestUtils.getAvailablePort();
    QueryServer serverB = startQueryServer(SERVER_B_ID, portB, rejectMisrouted);
    _addressX.switchTo(portB);
    int portY = QueryTestUtils.getAvailablePort();
    QueryServer restartedServerA = startQueryServer(SERVER_A_ID, portY, rejectMisrouted);
    _dns.map(SERVER_A, portY);
    return new Swap(serverB, restartedServerA);
  }

  private static class Swap {
    final QueryServer _serverB;
    final QueryServer _restartedServerA;

    Swap(QueryServer serverB, QueryServer restartedServerA) {
      _serverB = serverB;
      _restartedServerA = restartedServerA;
    }
  }

  private QueryServer startQueryServer(String instanceId, int port, boolean rejectMisrouted) {
    PinotConfiguration config = new PinotConfiguration(
        Map.of(CommonConstants.MultiStageQueryRunner.KEY_OF_REJECT_MISROUTED_REQUESTS, rejectMisrouted));
    QueryServer queryServer = spy(new QueryServer(config, instanceId, port, mock(QueryRunner.class), null, null,
        ThreadAccountantUtils.getNoOpAccountant()));
    queryServer.start();
    _queryServers.add(queryServer);
    return queryServer;
  }

  private void stopQueryServer(QueryServer queryServer) {
    queryServer.shutdown();
    _queryServers.remove(queryServer);
  }

  private static Worker.QueryRequest request() {
    return Worker.QueryRequest.newBuilder()
        .setMetadata(QueryPlanSerDeUtils.toProtoProperties(
            Map.of(MetadataKeys.REQUEST_ID, "1", QueryOptionKey.TIMEOUT_MS, "10000")))
        .build();
  }

  private AsyncResponse<Worker.QueryResponse> submit(QueryServerInstance target)
      throws Exception {
    CompletableFuture<AsyncResponse<Worker.QueryResponse>> future = new CompletableFuture<>();
    _queryDispatcher.getOrCreateDispatchClient(target)
        .submit(request(), target, Deadline.after(10, TimeUnit.SECONDS), future::complete);
    return future.get(10, TimeUnit.SECONDS);
  }

  /// Sends a request of the given kind to A, and returns its error, or `null` if it succeeded.
  @Nullable
  private Throwable sendRequest(String requestKind)
      throws Exception {
    DispatchClient client = _queryDispatcher.getOrCreateDispatchClient(TARGET_A);
    Deadline deadline = Deadline.after(10, TimeUnit.SECONDS);
    CompletableFuture<Throwable> error = new CompletableFuture<>();
    switch (requestKind) {
      case "cancel":
        client.cancel(1L, TARGET_A, deadline, response -> error.complete(response.getThrowable()));
        break;
      case "explain":
        client.explain(request(), TARGET_A, deadline, response -> error.complete(response.getThrowable()));
        break;
      case "submitWithStream":
        client.submitWithStream(request(), TARGET_A, deadline, new StreamingQuerySession(1L, 1), 1,
            (response, t) -> error.complete(t));
        break;
      default:
        throw new IllegalArgumentException("Unknown request kind: " + requestKind);
    }
    return error.get(10, TimeUnit.SECONDS);
  }

  private void processResults(AsyncResponse<Worker.QueryResponse> response)
      throws Exception {
    BlockingQueue<AsyncResponse<Worker.QueryResponse>> responses = new ArrayBlockingQueue<>(1);
    responses.add(response);
    _queryDispatcher.processResults(1L, 1, (r, server) -> {
    }, Deadline.after(10, TimeUnit.SECONDS), responses);
  }
}

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
package org.apache.pinot.core.transport;

import io.netty.channel.Channel;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class AsyncQueryResponseTest {
  private static final ServerRoutingInstance SERVER_1 =
      new ServerRoutingInstance("localhost", 11001, TableType.OFFLINE);
  private static final ServerRoutingInstance SERVER_2 =
      new ServerRoutingInstance("localhost", 11002, TableType.OFFLINE);

  private AsyncQueryResponse newResponse(Set<ServerRoutingInstance> servers, long timeoutMs,
      boolean skipUnavailableServers) {
    return new AsyncQueryResponse(mock(QueryRouter.class), 1L, servers, System.currentTimeMillis(), timeoutMs,
        mock(ServerRoutingStatsManager.class), skipUnavailableServers);
  }

  @Test
  public void testMarkServerUnavailableSkipsUnderFlag() {
    AsyncQueryResponse response = newResponse(Set.of(SERVER_1, SERVER_2), 10_000L, true);

    boolean skipped = response.markServerUnavailable(SERVER_1, new RuntimeException("channel inactive"));

    assertTrue(skipped);
    // The skip path must not fail the query: no exception, status not moved to FAILED.
    assertNull(response.getException());
    assertEquals(response.getStatus(), QueryResponse.Status.IN_PROGRESS);
    // The down server is recorded for the failure detector.
    assertEquals(response.getFailedServer(), SERVER_1);
  }

  @Test
  public void testMarkServerCancelledAlwaysFailsEvenWithSkip() {
    // Even with skipUnavailableServers=true, a broker-initiated OOM cancellation must fail the whole query. This guards
    // against a future refactor silently making OOM honor the skip flag.
    AsyncQueryResponse response = newResponse(Set.of(SERVER_1, SERVER_2), 10_000L, true);

    response.cancelQuery(SERVER_1,
        new QueryCancelledException("Query cancelled as broker is out of direct memory"));

    assertEquals(response.getStatus(), QueryResponse.Status.FAILED);
    assertNotNull(response.getException());
    assertEquals(response.getFailedServer(), SERVER_1);
  }

  @Test
  public void testMarkServerUnavailableWithoutFlagFails() {
    // Genuine unavailability but the flag is off: the whole query must still fail.
    AsyncQueryResponse response = newResponse(Set.of(SERVER_1, SERVER_2), 10_000L, false);

    boolean skipped = response.markServerUnavailable(SERVER_1, new RuntimeException("channel inactive"));

    assertFalse(skipped);
    assertEquals(response.getStatus(), QueryResponse.Status.FAILED);
    assertNotNull(response.getException());
  }

  @Test
  public void testMarkServerUnavailableIsIdempotent()
      throws Exception {
    // A server can be reported down twice for the same query
    AsyncQueryResponse response = newResponse(Set.of(SERVER_1, SERVER_2), 500L, true);

    assertTrue(response.markServerUnavailable(SERVER_1, new RuntimeException("write failure")));
    // Second report for the same server is a no-op for the latch.
    assertFalse(response.markServerUnavailable(SERVER_1, new RuntimeException("channel inactive")));

    // SERVER_2 never responds
    Map<ServerRoutingInstance, ServerResponse> responses = response.getFinalResponses();
    assertEquals(responses.size(), 2);
    assertEquals(response.getStatus(), QueryResponse.Status.TIMED_OUT);
    assertNull(response.getException());
    assertEquals(response.getFailedServer(), SERVER_1);
  }

  @Test
  public void testSkipServerResponseThenUnavailableIsIdempotent()
      throws Exception {
    AsyncQueryResponse response = newResponse(Set.of(SERVER_1, SERVER_2), 500L, true);

    response.skipServerResponse(SERVER_1);
    // The later channel-inactive report for the same server must be a no-op for the latch.
    assertFalse(response.markServerUnavailable(SERVER_1, new RuntimeException("channel inactive")));

    // Only SERVER_1's single slot was released; SERVER_2 never responds, so the query must TIME OUT, not COMPLETE.
    Map<ServerRoutingInstance, ServerResponse> responses = response.getFinalResponses();
    assertEquals(responses.size(), 2);
    assertEquals(response.getStatus(), QueryResponse.Status.TIMED_OUT);
    assertNull(response.getException());
    // The send-time skip also records the server for quarantine.
    assertEquals(response.getFailedServer(), SERVER_1);
  }
  /// A channel going down only affects the query written to it; a request whose channel is unknown is affected by
  /// any channel of the server, as before.
  @Test
  public void testMarkServerUnavailableIsScopedToTheRequestChannel() {
    Channel requestChannel = mock(Channel.class);
    Channel otherChannel = mock(Channel.class);
    AsyncQueryResponse response = newResponse(Set.of(SERVER_1, SERVER_2), 10_000L, false);
    response.markRequestChannel(SERVER_1, requestChannel);
    response.markRequestSubmitted(SERVER_1);
    response.markRequestSubmitted(SERVER_2);

    assertFalse(response.markServerUnavailable(SERVER_1, otherChannel, new RuntimeException("other"), true));
    assertEquals(response.getStatus(), QueryResponse.Status.IN_PROGRESS);
    assertNull(response.getFailedServer());

    // Unknown request channel: any channel of the server counts
    response.markServerUnavailable(SERVER_2, otherChannel, new RuntimeException("other"), true);
    assertEquals(response.getStatus(), QueryResponse.Status.FAILED);
    assertEquals(response.getFailedServer(), SERVER_2);

    response = newResponse(Set.of(SERVER_1), 10_000L, false);
    response.markRequestChannel(SERVER_1, requestChannel);
    response.markRequestSubmitted(SERVER_1);
    response.markServerUnavailable(SERVER_1, requestChannel, new RuntimeException("own"), true);
    assertEquals(response.getStatus(), QueryResponse.Status.FAILED);
    assertEquals(response.getFailedServer(), SERVER_1);
  }

  /// A close the broker itself decided on fails or skips the query like any other, but does not record the server
  /// for the failure detector.
  @Test
  public void testBrokerInitiatedCloseDoesNotBlameTheServer() {
    AsyncQueryResponse response = newResponse(Set.of(SERVER_1), 10_000L, false);
    response.markRequestSubmitted(SERVER_1);
    response.markServerUnavailable(SERVER_1, null, new RuntimeException("replaced"), false);
    assertEquals(response.getStatus(), QueryResponse.Status.FAILED);
    assertNotNull(response.getException());
    assertNull(response.getFailedServer());

    response = newResponse(Set.of(SERVER_1, SERVER_2), 10_000L, true);
    response.markRequestSubmitted(SERVER_1);
    assertTrue(response.markServerUnavailable(SERVER_1, null, new RuntimeException("replaced"), false));
    assertEquals(response.getStatus(), QueryResponse.Status.IN_PROGRESS);
    assertNull(response.getFailedServer());

    // A request whose channel is not known yet is about to be written, possibly to the channel replacing the closed
    // one: the broker's own close leaves it alone, while a genuine failure of the server still takes it down
    Channel closedChannel = mock(Channel.class);
    response = newResponse(Set.of(SERVER_1), 10_000L, false);
    assertFalse(response.markServerUnavailable(SERVER_1, closedChannel, new RuntimeException("replaced"), false));
    assertEquals(response.getStatus(), QueryResponse.Status.IN_PROGRESS);
    response.markServerUnavailable(SERVER_1, closedChannel, new RuntimeException("inactive"), true);
    assertEquals(response.getStatus(), QueryResponse.Status.FAILED);
    assertEquals(response.getFailedServer(), SERVER_1);
  }

  /// A timeout hands every server the request reached but that never answered to the transport, with the time the
  /// request was written, and blames nobody on the spot. Servers the request never reached are not handed over.
  @Test
  public void testTimeoutRecordsUnansweredServers()
      throws Exception {
    QueryRouter queryRouter = mock(QueryRouter.class);
    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse response =
        new AsyncQueryResponse(queryRouter, 1L, Set.of(SERVER_1, SERVER_2), startTimeMs, 50L,
            mock(ServerRoutingStatsManager.class), false);
    // SERVER_2 is never marked submitted: its send failed before anything was written
    response.markRequestSubmitted(SERVER_1);

    response.getFinalResponses();

    assertEquals(response.getStatus(), QueryResponse.Status.TIMED_OUT);
    assertNull(response.getFailedServer());
    assertNull(response.getException());
    verify(queryRouter).recordServerTimeout(eq(SERVER_1), anyLong(), any());
    verify(queryRouter, never()).recordServerTimeout(eq(SERVER_2), anyLong(), any());
  }

  @Test
  public void testAnsweredQueryRecordsNoTimeout()
      throws Exception {
    QueryRouter queryRouter = mock(QueryRouter.class);
    AsyncQueryResponse response =
        new AsyncQueryResponse(queryRouter, 1L, Set.of(SERVER_1), System.currentTimeMillis(), 10_000L,
            mock(ServerRoutingStatsManager.class), false);
    response.markRequestSubmitted(SERVER_1);
    response.receiveDataTable(SERVER_1, mock(DataTable.class), 10, 1);

    response.getFinalResponses();

    assertEquals(response.getStatus(), QueryResponse.Status.COMPLETED);
    verify(queryRouter, never()).recordServerTimeout(eq(SERVER_1), anyLong(), any());
    assertNull(response.getFailedServer());
  }
}

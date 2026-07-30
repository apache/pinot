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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.exception.QueryErrorCode;


/**
 * The {@code AsyncQueryResponse} class represents an asynchronous query response.
 */
@ThreadSafe
public class AsyncQueryResponse implements QueryResponse {
  private final QueryRouter _queryRouter;
  private final long _requestId;
  private final AtomicReference<Status> _status = new AtomicReference<>(Status.IN_PROGRESS);
  private final AtomicInteger _numServersResponded = new AtomicInteger();
  private final ConcurrentHashMap<ServerRoutingInstance, ServerResponse> _responseMap;
  private final CountDownLatch _countDownLatch;
  private final long _maxEndTimeMs;
  private final long _timeoutMs;
  private final ServerRoutingStatsManager _serverRoutingStatsManager;
  private final boolean _skipUnavailableServers;
  // Servers whose latch slot has already been released via a down-path (skipServerResponse or markServerUnavailable).
  // A server can be reported down more than once for the same query
  private final Set<ServerRoutingInstance> _countedDownServers = ConcurrentHashMap.newKeySet();

  private volatile ServerRoutingInstance _failedServer;
  private volatile Exception _exception;

  public AsyncQueryResponse(QueryRouter queryRouter, long requestId, Set<ServerRoutingInstance> serversQueried,
      long startTimeMs, long timeoutMs, ServerRoutingStatsManager serverRoutingStatsManager,
      boolean skipUnavailableServers) {
    _queryRouter = queryRouter;
    _requestId = requestId;
    _skipUnavailableServers = skipUnavailableServers;
    int numServersQueried = serversQueried.size();
    _responseMap = new ConcurrentHashMap<>(HashUtil.getHashMapCapacity(numServersQueried));
    _serverRoutingStatsManager = serverRoutingStatsManager;
    for (ServerRoutingInstance serverRoutingInstance : serversQueried) {
      // Record stats related to query submission just before sending the request. Otherwise, if the response is
      // received immediately, there's a possibility of updating query response stats before updating query
      // submission stats.
      _serverRoutingStatsManager.recordStatsForQuerySubmission(requestId, serverRoutingInstance.getInstanceId());
      _responseMap.put(serverRoutingInstance, new ServerResponse(startTimeMs));
    }
    _countDownLatch = new CountDownLatch(numServersQueried);
    _timeoutMs = timeoutMs;
    _maxEndTimeMs = startTimeMs + timeoutMs;
  }

  @Override
  public Status getStatus() {
    return _status.get();
  }

  @Override
  public int getNumServersResponded() {
    return _numServersResponded.get();
  }

  @Override
  public Map<ServerRoutingInstance, ServerResponse> getCurrentResponses() {
    return _responseMap;
  }

  @Override
  public Map<ServerRoutingInstance, ServerResponse> getFinalResponses()
      throws InterruptedException {
    try {
      boolean finish = _countDownLatch.await(_maxEndTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      _status.compareAndSet(Status.IN_PROGRESS, finish ? Status.COMPLETED : Status.TIMED_OUT);
      return _responseMap;
    } finally {
      // Update ServerRoutingStats for query completion. This is done here to ensure that the stats are updated for
      // servers even if the query times out or if servers have not responded.
      for (Map.Entry<ServerRoutingInstance, ServerResponse> entry : _responseMap.entrySet()) {
        ServerResponse response = entry.getValue();
        long latency;

        // If server has not responded or if the server response has exceptions, the latency is set to timeout
        if (hasServerNotResponded(response) || hasServerReturnedExceptions(response)) {
          latency = _timeoutMs;
        } else {
          latency = response.getResponseDelayMs();
        }
        _serverRoutingStatsManager.recordStatsUponResponseArrival(_requestId, entry.getKey().getInstanceId(), latency);
      }

      _queryRouter.markQueryDone(_requestId);
    }
  }

  private boolean hasServerReturnedExceptions(ServerResponse response) {
    if (response.getDataTable() != null && response.getDataTable().getExceptions().size() > 0) {
      DataTable dataTable = response.getDataTable();
      Map<Integer, String> exceptions = dataTable.getExceptions();

      // If Server response has exceptions in Datatable set the latency for timeout value.
      for (Map.Entry<Integer, String> exception : exceptions.entrySet()) {
        // Check if the exceptions received are server side exceptions
        if (!QueryErrorCode.fromErrorCode(exception.getKey()).isClientError()) {
          return true;
        }
      }
      return false;
    }
    return false;
  }

  private boolean hasServerNotResponded(ServerResponse response) {
    // ServerResponse returns -1 if responseDelayMs is not set. This indicates that a response was not received
    // from the server. Hence we set the latency to the timeout value.
    return response == null || response.getResponseDelayMs() < 0;
  }

  @Override
  public String getServerStats() {
    StringBuilder stringBuilder = new StringBuilder(
        "(Server=SubmitDelayMs,ResponseDelayMs,ResponseSize,DeserializationTimeMs,RequestSentDelayMs)");
    for (Map.Entry<ServerRoutingInstance, ServerResponse> entry : _responseMap.entrySet()) {
      stringBuilder.append(';').append(entry.getKey().getShortName()).append('=').append(entry.getValue().toString());
    }
    return stringBuilder.toString();
  }

  @Override
  public long getServerResponseDelayMs(ServerRoutingInstance serverRoutingInstance) {
    return _responseMap.get(serverRoutingInstance).getResponseDelayMs();
  }

  @Nullable
  @Override
  public ServerRoutingInstance getFailedServer() {
    return _failedServer;
  }

  @Override
  public Exception getException() {
    return _exception;
  }

  @Override
  public long getRequestId() {
    return _requestId;
  }

  @Override
  public long getTimeoutMs() {
    return _timeoutMs;
  }

  void markRequestSubmitted(ServerRoutingInstance serverRoutingInstance) {
    _responseMap.get(serverRoutingInstance).markRequestSubmitted();
  }

  void markRequestSent(ServerRoutingInstance serverRoutingInstance, int requestSentLatencyMs) {
    _responseMap.get(serverRoutingInstance).markRequestSent(requestSentLatencyMs);
  }

  void receiveDataTable(ServerRoutingInstance serverRoutingInstance, DataTable dataTable, int responseSize,
      int deserializationTimeMs) {
    ServerResponse response = _responseMap.get(serverRoutingInstance);
    response.receiveDataTable(dataTable, responseSize, deserializationTimeMs);

    _numServersResponded.getAndIncrement();
    _countDownLatch.countDown();
  }

  void markQueryFailed(ServerRoutingInstance serverRoutingInstance, Exception exception) {
    _status.set(Status.FAILED);
    _failedServer = serverRoutingInstance;
    _countedDownServers.add(serverRoutingInstance);
    _exception = exception;
    int count = (int) _countDownLatch.getCount();
    for (int i = 0; i < count; i++) {
      _countDownLatch.countDown();
    }
  }

  /**
   * Marks a server as unavailable while the query is in flight - its Netty channel went inactive or a request write
   * to it failed. This is a genuine server-unavailability event that is eligible for {@code skipUnavailableServers}.
   * NOTE: the server might not have been hit by this query. Only acts if the query was sent to the server and the
   * server has not responded yet.
   */
  boolean markServerUnavailable(ServerRoutingInstance serverRoutingInstance, Exception exception) {
    if (!shouldActOnServerDown(serverRoutingInstance)) {
      return false;
    }
    if (_skipUnavailableServers) {
      // Best-effort: degrade to partial results. Record the down server so the failure detector can quarantine it from
      // routing, but do NOT set the query-global exception/status (no BROKER_REQUEST_SEND error, query stays
      // COMPLETED)
      if (_countedDownServers.add(serverRoutingInstance)) {
        _failedServer = serverRoutingInstance;
        _countDownLatch.countDown();
        return true;
      }
      return false;
    }
    markQueryFailed(serverRoutingInstance, exception);
    return false;
  }

  /**
   * Cancels the query because the broker is shedding it (eg. direct-memory OOM), not because a server is unavailable
   */
  void markServerCancelled(ServerRoutingInstance serverRoutingInstance, Exception exception) {
    if (!shouldActOnServerDown(serverRoutingInstance)) {
      return;
    }
    markQueryFailed(serverRoutingInstance, exception);
  }

  /**
   * Returns {@code true} if a server-down event should be acted on: the server was queried by this response and has not
   * responded yet
   */
  private boolean shouldActOnServerDown(ServerRoutingInstance serverRoutingInstance) {
    ServerResponse serverResponse = _responseMap.get(serverRoutingInstance);
    return serverResponse != null && serverResponse.getDataTable() == null;
  }

  /**
   * Releases the latch slot for a server that was skipped at request-submission time (the send failed and the query was
   * submitted with {@code skipUnavailableServers=true}), so the query does not wait for a response that will never
   * come.
   */
  void skipServerResponse(ServerRoutingInstance serverRoutingInstance) {
    if (_countedDownServers.add(serverRoutingInstance)) {
      _failedServer = serverRoutingInstance;
      _countDownLatch.countDown();
    }
  }
}

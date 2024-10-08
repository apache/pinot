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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;


/**
 * The {@code AsyncQueryResponse} class represents an asynchronous query response.
 */
@ThreadSafe
public class AsyncQueryResponse implements QueryResponse {
  private final QueryRouter _queryRouter;
  private final long _requestId;
  private final AtomicReference<Status> _status = new AtomicReference<>(Status.IN_PROGRESS);
  private final AtomicInteger _numServersResponded = new AtomicInteger();
  private final ConcurrentHashMap<ServerRoutingInstance, ConcurrentHashMap<String, ServerResponse>> _responses;
  private final CountDownLatch _countDownLatch;
  private final long _maxEndTimeMs;
  private final long _timeoutMs;
  private final ServerRoutingStatsManager _serverRoutingStatsManager;

  private volatile ServerRoutingInstance _failedServer;
  private volatile Exception _exception;

  public AsyncQueryResponse(QueryRouter queryRouter, long requestId,
      Map<ServerRoutingInstance, List<InstanceRequest>> requestMap, long startTimeMs, long timeoutMs,
      ServerRoutingStatsManager serverRoutingStatsManager) {
    _queryRouter = queryRouter;
    _requestId = requestId;
    _responses = new ConcurrentHashMap<>();
    _serverRoutingStatsManager = serverRoutingStatsManager;
    int numQueriesIssued = 0;
    for (Map.Entry<ServerRoutingInstance, List<InstanceRequest>> serverRequests : requestMap.entrySet()) {
      for (InstanceRequest request : serverRequests.getValue()) {
        // Record stats related to query submission just before sending the request. Otherwise, if the response is
        // received immediately, there's a possibility of updating query response stats before updating query
        // submission stats.
        _serverRoutingStatsManager.recordStatsForQuerySubmission(requestId, serverRequests.getKey().getInstanceId());

        _responses.computeIfAbsent(serverRequests.getKey(), k -> new ConcurrentHashMap<>())
            .put(request.getQuery().getPinotQuery().getDataSource().getTableName(), new ServerResponse(startTimeMs));
        numQueriesIssued++;
      }
    }

    _countDownLatch = new CountDownLatch(numQueriesIssued);
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

  private Map<ServerRoutingInstance, List<ServerResponse>> getFlatResponses() {
    Map<ServerRoutingInstance, List<ServerResponse>> flattened = new HashMap<>();
    for (Map.Entry<ServerRoutingInstance, ConcurrentHashMap<String, ServerResponse>> serverResponses :
        _responses.entrySet()) {
      ServerRoutingInstance serverRoutingInstance = serverResponses.getKey();

      for (ServerResponse serverResponse : serverResponses.getValue().values()) {
        flattened.computeIfAbsent(serverRoutingInstance, k -> new ArrayList<>()).add(serverResponse);
      }
    }

    return flattened;
  }

  @Override
  public Map<ServerRoutingInstance, List<ServerResponse>> getCurrentResponses() {
    return getFlatResponses();
  }

  @Override
  public Map<ServerRoutingInstance, List<ServerResponse>> getFinalResponses()
      throws InterruptedException {
    Map<ServerRoutingInstance, List<ServerResponse>> flatResponses = getFlatResponses();
    try {
      boolean finish = _countDownLatch.await(_maxEndTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      _status.compareAndSet(Status.IN_PROGRESS, finish ? Status.COMPLETED : Status.TIMED_OUT);
      return flatResponses;
    } finally {
      // Update ServerRoutingStats for query completion. This is done here to ensure that the stats are updated for
      // servers even if the query times out or if servers have not responded.
      for (Map.Entry<ServerRoutingInstance, List<ServerResponse>> serverResponses : flatResponses.entrySet()) {
        for (ServerResponse response : serverResponses.getValue()) {
          // ServerResponse returns -1 if responseDelayMs is not set. This indicates that a response was not received
          // from the server. Hence we set the latency to the timeout value.
          long latency =
              (response != null && response.getResponseDelayMs() >= 0) ? response.getResponseDelayMs() : _timeoutMs;
          _serverRoutingStatsManager.recordStatsUponResponseArrival(_requestId,
              serverResponses.getKey().getInstanceId(), latency);
        }
      }
      _queryRouter.markQueryDone(_requestId);
    }
  }

  @Override
  public String getServerStats() {
    StringBuilder stringBuilder = new StringBuilder(
        "(Server=SubmitDelayMs,ResponseDelayMs,ResponseSize,DeserializationTimeMs,RequestSentDelayMs)");
    for (Map.Entry<ServerRoutingInstance, ConcurrentHashMap<String, ServerResponse>> serverResponses :
        _responses.entrySet()) {
      for (Map.Entry<String, ServerResponse> responsePair : serverResponses.getValue().entrySet()) {
        ServerRoutingInstance serverRoutingInstance = serverResponses.getKey();
        stringBuilder.append(';').append(serverRoutingInstance.getShortName()).append('=')
            .append(responsePair.getValue().toString());
      }
    }
    return stringBuilder.toString();
  }

  @Override
  public long getServerResponseDelayMs(ServerRoutingInstance serverRoutingInstance) {
    // TODO(egalpin): How to get tableName here?
    return -1L;
//    return _responseMap.get(serverRoutingInstance).getResponseDelayMs();
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

  void markRequestSubmitted(ServerRoutingInstance serverRoutingInstance, InstanceRequest instanceRequest) {
    _responses.get(serverRoutingInstance).get(instanceRequest.getQuery().getPinotQuery().getDataSource().getTableName())
        .markRequestSubmitted();
  }

  void markRequestSent(ServerRoutingInstance serverRoutingInstance, InstanceRequest instanceRequest,
      int requestSentLatencyMs) {
    _responses.get(serverRoutingInstance).get(instanceRequest.getQuery().getPinotQuery().getDataSource().getTableName())
        .markRequestSent(requestSentLatencyMs);
  }

  void receiveDataTable(ServerRoutingInstance serverRoutingInstance, DataTable dataTable,
      int responseSize, int deserializationTimeMs) {
    String tableName = dataTable.getMetadata().get(DataTable.MetadataKey.TABLE.getName());
    // tableName can be null but only in the case of version rollout. tableName will only be null when servers are not
    // yet running the version where tableName is included in DataTable metadata. For the time being, it is safe to
    // assume that a given server will have only been sent 1 or 2 requests (REALTIME, OFFLINE, or both).  We can simply
    // iterate through responses associated with a server and make use of the first response with null data table.
    // TODO(egalpin): The null handling for tableName can and should be removed in a later release.
    if (tableName == null) {
      for (ServerResponse serverResponse : _responses.get(serverRoutingInstance).values()) {
        if (serverResponse.getDataTable() == null) {
          serverResponse.receiveDataTable(dataTable, responseSize, deserializationTimeMs);
          break;
        }
      }
    } else {
      _responses.get(serverRoutingInstance).get(tableName).receiveDataTable(dataTable, responseSize, deserializationTimeMs);
    }

    _countDownLatch.countDown();
    _numServersResponded.getAndIncrement();
  }

  void markQueryFailed(ServerRoutingInstance serverRoutingInstance, Exception exception) {
    _status.set(Status.FAILED);
    _failedServer = serverRoutingInstance;
    _exception = exception;
    int count = (int) _countDownLatch.getCount();
    for (int i = 0; i < count; i++) {
      _countDownLatch.countDown();
    }
  }

  /**
   * NOTE: the server might not be hit by the query. Only fail the query if the query was sent to the server and the
   * server hasn't responded yet.
   */
  void markServerDown(ServerRoutingInstance serverRoutingInstance, Exception exception) {
    // TODO(egalpin): how to mark servers down under the assumption that multiple queries
    // to the same server are valid?
    Map<String, ServerResponse> serverResponses = _responses.get(serverRoutingInstance);
    for (ServerResponse serverResponse : serverResponses.values()) {
      if (serverResponse != null && serverResponse.getDataTable() == null) {
        markQueryFailed(serverRoutingInstance, exception);
      }
    }
  }

  /**
   * Wait for one less server response. This is used when the server is skipped, as
   * query submission will have failed we do not want to wait for the response.
   */
  void skipServerResponse() {
    _countDownLatch.countDown();
  }
}

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

  private volatile ServerRoutingInstance _failedServer;
  private volatile Exception _exception;

  public AsyncQueryResponse(QueryRouter queryRouter, long requestId, Set<ServerRoutingInstance> serversQueried,
      long startTimeMs, long timeoutMs) {
    _queryRouter = queryRouter;
    _requestId = requestId;
    int numServersQueried = serversQueried.size();
    _responseMap = new ConcurrentHashMap<>(numServersQueried);
    for (ServerRoutingInstance serverRoutingInstance : serversQueried) {
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
      _queryRouter.markQueryDone(_requestId);
    }
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
    _responseMap.get(serverRoutingInstance).receiveDataTable(dataTable, responseSize, deserializationTimeMs);
    _numServersResponded.getAndIncrement();
    _countDownLatch.countDown();
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
    ServerResponse serverResponse = _responseMap.get(serverRoutingInstance);
    if (serverResponse != null && serverResponse.getDataTable() == null) {
      markQueryFailed(serverRoutingInstance, exception);
    }
  }
}

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
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.utils.DataTable;


/**
 * The {@code AsyncQueryResponse} class represents an asynchronous query response.
 * <p>Call {@link #getResponse()} to get the query response asynchronously.
 */
@ThreadSafe
public class AsyncQueryResponse {
  private final QueryRouter _queryRouter;
  private final long _requestId;
  private final ConcurrentHashMap<ServerRoutingInstance, ServerResponse> _responseMap;
  private final CountDownLatch _countDownLatch;
  private final long _maxEndTimeMs;

  private volatile Exception _brokerRequestSendException;

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
    _maxEndTimeMs = startTimeMs + timeoutMs;
  }

  /**
   * Waits until the query is done and returns a map from the server to the response.
   */
  public Map<ServerRoutingInstance, ServerResponse> getResponse()
      throws InterruptedException {
    try {
      _countDownLatch.await(_maxEndTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      return _responseMap;
    } finally {
      _queryRouter.markQueryDone(_requestId);
    }
  }

  /**
   * Returns the statistics for the servers the query sent to.
   * <p>Should be called after calling {@link #getResponse()}.
   */
  public String getStats() {
    StringBuilder stringBuilder =
        new StringBuilder("(Server=SubmitDelayMs,ResponseDelayMs,ResponseSize,DeserializationTimeMs,RequestSentDelayMs)");
    for (Map.Entry<ServerRoutingInstance, ServerResponse> entry : _responseMap.entrySet()) {
      stringBuilder.append(';').append(entry.getKey().getShortName()).append('=').append(entry.getValue().toString());
    }
    return stringBuilder.toString();
  }

  void markRequestSubmitted(ServerRoutingInstance serverRoutingInstance) {
    _responseMap.get(serverRoutingInstance).markRequestSubmitted();
  }

  void markRequestSent(ServerRoutingInstance serverRoutingInstance, long requestSentLatencyMs) {
    _responseMap.get(serverRoutingInstance).markRequestSent(requestSentLatencyMs);
  }

  void receiveDataTable(ServerRoutingInstance serverRoutingInstance, DataTable dataTable, int responseSize,
      int deserializationTimeMs) {
    _responseMap.get(serverRoutingInstance).receiveDataTable(dataTable, responseSize, deserializationTimeMs);
    _countDownLatch.countDown();
  }

  void markQueryFailed() {
    int count = (int) _countDownLatch.getCount();
    for (int i = 0; i < count; i++) {
      _countDownLatch.countDown();
    }
  }

  /**
   * NOTE: the server might not be hit by the query. Only fail the query if the query was sent to the server and the
   * server hasn't responded yet.
   */
  void markServerDown(ServerRoutingInstance serverRoutingInstance) {
    ServerResponse serverResponse = _responseMap.get(serverRoutingInstance);
    if (serverResponse != null && serverResponse.getDataTable() == null) {
      markQueryFailed();
    }
  }

  public Exception getBrokerRequestSendException() {
    return _brokerRequestSendException;
  }

  void setBrokerRequestSendException(Exception brokerRequestSendException) {
    _brokerRequestSendException = brokerRequestSendException;
  }
}

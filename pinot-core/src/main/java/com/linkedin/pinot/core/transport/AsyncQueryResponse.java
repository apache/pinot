/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.transport;

import com.linkedin.pinot.common.utils.DataTable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;


/**
 * The {@code AsyncQueryResponse} class represents an asynchronous query response.
 * <p>Call {@link #getResponse()} to get the query response asynchronously.
 */
@ThreadSafe
public class AsyncQueryResponse {
  private final QueryRouter _queryRouter;
  private final long _requestId;
  private final ConcurrentHashMap<Server, ServerResponse> _responseMap;
  private final CountDownLatch _countDownLatch;
  private final long _maxEndTimeMs;

  public AsyncQueryResponse(QueryRouter queryRouter, long requestId, Set<Server> serversQueried, long startTimeMs,
      long timeoutMs) {
    _queryRouter = queryRouter;
    _requestId = requestId;
    int numServersQueried = serversQueried.size();
    _responseMap = new ConcurrentHashMap<>(numServersQueried);
    for (Server server : serversQueried) {
      _responseMap.put(server, new ServerResponse(startTimeMs));
    }
    _countDownLatch = new CountDownLatch(numServersQueried);
    _maxEndTimeMs = startTimeMs + timeoutMs;
  }

  /**
   * Waits until the query is done and returns a map from the server to the response.
   */
  public Map<Server, ServerResponse> getResponse() throws InterruptedException {
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
        new StringBuilder("(Server=SubmitDelayMs,ResponseDelayMs,ResponseSize,DeserializationTimeMs)");
    for (Map.Entry<Server, ServerResponse> entry : _responseMap.entrySet()) {
      stringBuilder.append(';').append(entry.getKey().getShortName()).append('=').append(entry.getValue().toString());
    }
    return stringBuilder.toString();
  }

  void markRequestSubmitted(Server server) {
    _responseMap.get(server).markRequestSubmitted();
  }

  void receiveDataTable(Server server, DataTable dataTable, long responseSize, long deserializationTimeMs) {
    _responseMap.get(server).receiveDataTable(dataTable, responseSize, deserializationTimeMs);
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
  void markServerDown(Server server) {
    ServerResponse serverResponse = _responseMap.get(server);
    if (serverResponse != null && serverResponse.getDataTable() == null) {
      markQueryFailed();
    }
  }
}

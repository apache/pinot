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

import io.grpc.stub.StreamObserver;
import javax.annotation.Nullable;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.QueryServerInstance;


/**
 * Response returned by {@link DispatchClient} asynchronously using a callback. If the {@link StreamObserver#onError}
 * was called (i.e. the dispatch failed), then the exception is stored in the response and the caller should use
 * {@link #getThrowable()} to check if it is null.
 */
class AsyncQueryDispatchResponse {
  private final QueryServerInstance _virtualServer;
  private final int _stageId;
  private final Worker.QueryResponse _queryResponse;
  private final Throwable _throwable;

  public AsyncQueryDispatchResponse(QueryServerInstance virtualServer, int stageId, Worker.QueryResponse queryResponse,
      @Nullable Throwable throwable) {
    _virtualServer = virtualServer;
    _stageId = stageId;
    _queryResponse = queryResponse;
    _throwable = throwable;
  }

  public QueryServerInstance getVirtualServer() {
    return _virtualServer;
  }

  public int getStageId() {
    return _stageId;
  }

  public Worker.QueryResponse getQueryResponse() {
    return _queryResponse;
  }

  @Nullable
  public Throwable getThrowable() {
    return _throwable;
  }
}

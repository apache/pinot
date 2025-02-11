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
import org.apache.pinot.query.routing.QueryServerInstance;


/**
 * Response returned by {@link DispatchClient} asynchronously using a callback. If the {@link StreamObserver#onError}
 * was called (i.e. the dispatch failed), then the exception is stored in the response and the caller should use
 * {@link #getThrowable()} to check if it is null.
 */
class AsyncResponse<E> {
  private final QueryServerInstance _serverInstance;
  private final E _response;
  private final Throwable _throwable;

  public AsyncResponse(QueryServerInstance serverInstance, @Nullable E response, @Nullable Throwable throwable) {
    _serverInstance = serverInstance;
    _response = response;
    _throwable = throwable;
  }

  public QueryServerInstance getServerInstance() {
    return _serverInstance;
  }

  @Nullable
  public E getResponse() {
    return _response;
  }

  @Nullable
  public Throwable getThrowable() {
    return _throwable;
  }
}

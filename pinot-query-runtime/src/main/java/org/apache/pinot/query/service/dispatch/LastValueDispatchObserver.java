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
import java.util.function.Consumer;
import org.apache.pinot.query.routing.QueryServerInstance;


/**
 * A {@link StreamObserver} used by {@link DispatchClient} to obtain the last the response of an async Query Dispatch
 * call.
 */
class LastValueDispatchObserver<E> implements StreamObserver<E> {
  private final QueryServerInstance _serverInstance;
  private final Consumer<AsyncResponse<E>> _callback;

  private E _result;

  public LastValueDispatchObserver(QueryServerInstance serverInstance, Consumer<AsyncResponse<E>> callback) {
    _serverInstance = serverInstance;
    _callback = callback;
  }

  @Override
  public void onNext(E result) {
    _result = result;
  }

  @Override
  public void onError(Throwable throwable) {
    _callback.accept(new AsyncResponse<>(_serverInstance, null, throwable));
  }

  @Override
  public void onCompleted() {
    _callback.accept(new AsyncResponse<>(_serverInstance, _result, null));
  }
}

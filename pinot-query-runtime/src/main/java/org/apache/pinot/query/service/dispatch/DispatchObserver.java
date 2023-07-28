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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.service.QueryConfig;


/**
 * A {@link StreamObserver} used by {@link DispatchClient} to subscribe to the response of a async Query Dispatch call.
 */
class DispatchObserver implements StreamObserver<Worker.QueryResponse> {
  private final int _stageId;
  private final QueryServerInstance _virtualServer;
  private final CountDownLatch _latch = new CountDownLatch(1);

  private Worker.QueryResponse _queryResponse;
  private Throwable _throwable;

  public DispatchObserver(int stageId, QueryServerInstance virtualServer) {
    _stageId = stageId;
    _virtualServer = virtualServer;
  }

  @Override
  public void onNext(Worker.QueryResponse queryResponse) {
    _queryResponse = queryResponse;
    _latch.countDown();
  }

  @Override
  public void onError(Throwable throwable) {
    _throwable = throwable;
    _latch.countDown();
  }

  @Override
  public void onCompleted() {
    _latch.countDown();
  }

  public void await(long timeoutMs) throws Exception {
    boolean awaitSuccess = _latch.await(timeoutMs, TimeUnit.MILLISECONDS);
    if (_throwable != null) {
      throw new RuntimeException(String.format("Unable to dispatch query plan at stage %s on server %s",
          _stageId, _virtualServer), _throwable);
    } else if (_queryResponse == null || !awaitSuccess) {
      throw new TimeoutException(String.format("Unable to dispatch query plan at stage %s on server %s: ERROR: %s",
          _stageId, _virtualServer, "Timeout occurred or query submission unable to return"));
    } else {
      if (_queryResponse.containsMetadata(QueryConfig.KEY_OF_SERVER_RESPONSE_STATUS_ERROR)) {
        throw new RuntimeException(String.format("Unable to dispatch query plan at stage %s on server %s: ERROR: %s",
            _stageId, _virtualServer, _queryResponse.getMetadataOrDefault(
                QueryConfig.KEY_OF_SERVER_RESPONSE_STATUS_ERROR, "null")));
      }
    }
  }
}

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

import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.service.QueryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dispatches a query plan to given a {@link QueryServerInstance}. Each {@link DispatchClient} has its own gRPC Channel
 * and Client Stub.
 * TODO: It might be neater to implement pooling at the client level. Two options: (1) Pass a channel provider and
 *       let that take care of pooling. (2) Create a DispatchClient interface and implement pooled/non-pooled versions.
 */
class DispatchClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(DispatchClient.class);
  private static final StreamObserver<Worker.CancelResponse> NO_OP_CANCEL_STREAM_OBSERVER = new NoOpObserver();
  private final ManagedChannel _channel;
  private final PinotQueryWorkerGrpc.PinotQueryWorkerStub _dispatchStub;

  public DispatchClient(String host, int port) {
    _channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    _dispatchStub = PinotQueryWorkerGrpc.newStub(_channel);
  }

  public ManagedChannel getChannel() {
    return _channel;
  }

  public void submit(Worker.QueryRequest request, int stageId, QueryServerInstance virtualServer, Deadline deadline) {
    try {
      CompletableFuture<Void> future = new CompletableFuture<>();
      DispatchObserver dispatchObserver = new DispatchObserver(stageId, virtualServer, future);
      _dispatchStub.withDeadline(deadline).submit(request, dispatchObserver);
      future.get(deadline.timeRemaining(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      LOGGER.error("Query Dispatch failed!", t);
      Utils.rethrowException(t);
    }
  }

  public void cancel(long requestId) {
    try {
      Worker.CancelRequest cancelRequest = Worker.CancelRequest.newBuilder().setRequestId(requestId).build();
      _dispatchStub.cancel(cancelRequest, NO_OP_CANCEL_STREAM_OBSERVER);
    } catch (Exception e) {
      LOGGER.error("Query Cancellation failed!", e);
    }
  }

  private static class DispatchObserver implements StreamObserver<Worker.QueryResponse> {
    private static final Void VOID = null;
    private final int _stageId;
    private final QueryServerInstance _virtualServer;
    private final CompletableFuture<Void> _future;

    DispatchObserver(int stageId, QueryServerInstance virtualServer, CompletableFuture<Void> future) {
      _stageId = stageId;
      _virtualServer = virtualServer;
      _future = future;
    }

    @Override
    public void onNext(Worker.QueryResponse queryResponse) {
      if (queryResponse == null) {
        _future.completeExceptionally(new TimeoutException(String.format("Unable to dispatch query plan at stage %s on"
                + " server %s due to timeout.", _stageId, _virtualServer)));
      } else if (queryResponse.containsMetadata(QueryConfig.KEY_OF_SERVER_RESPONSE_STATUS_ERROR)) {
        _future.completeExceptionally(new RuntimeException(String.format("Unable to dispatch query plan at stage %s on"
                + " server %s: ERROR: %s", _stageId, _virtualServer,
            queryResponse.getMetadataOrDefault(QueryConfig.KEY_OF_SERVER_RESPONSE_STATUS_ERROR, "null"))));
      } else {
        _future.complete(VOID);
      }
    }

    @Override
    public void onError(Throwable t) {
      _future.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
      // do nothing.
    }
  }

  private static class NoOpObserver implements StreamObserver<Worker.CancelResponse> {
    @Override
    public void onNext(Worker.CancelResponse cancelResponse) {
    }

    @Override
    public void onError(Throwable throwable) {
    }

    @Override
    public void onCompleted() {
    }
  }
}

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
import java.util.function.Consumer;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.QueryServerInstance;
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
  private static final StreamObserver<Worker.CancelResponse> NO_OP_CANCEL_STREAM_OBSERVER = new CancelObserver();
  private final ManagedChannel _channel;
  private final PinotQueryWorkerGrpc.PinotQueryWorkerStub _dispatchStub;

  public DispatchClient(String host, int port) {
    _channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    _dispatchStub = PinotQueryWorkerGrpc.newStub(_channel);
  }

  public ManagedChannel getChannel() {
    return _channel;
  }

  public void submit(Worker.QueryRequest request, int stageId, QueryServerInstance virtualServer, Deadline deadline,
      Consumer<AsyncQueryDispatchResponse> callback) {
    try {
      _dispatchStub.withDeadline(deadline).submit(request, new DispatchObserver(stageId, virtualServer, callback));
    } catch (Exception e) {
      LOGGER.error("Query Dispatch failed at client-side", e);
      callback.accept(new AsyncQueryDispatchResponse(
          virtualServer, stageId, Worker.QueryResponse.getDefaultInstance(), e));
    }
  }

  public void cancel(long requestId) {
    try {
      Worker.CancelRequest cancelRequest = Worker.CancelRequest.newBuilder().setRequestId(requestId).build();
      _dispatchStub.cancel(cancelRequest, NO_OP_CANCEL_STREAM_OBSERVER);
    } catch (Exception e) {
      LOGGER.error("Query Cancellation failed at client-side", e);
    }
  }
}

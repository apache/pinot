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
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.grpc.ServerGrpcQueryClient;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.spi.query.QueryThreadContext;


/**
 * Dispatches a query plan to given a {@link QueryServerInstance}. Each {@link DispatchClient} has its own gRPC Channel
 * and Client Stub.
 * TODO: It might be neater to implement pooling at the client level. Two options: (1) Pass a channel provider and
 *       let that take care of pooling. (2) Create a DispatchClient interface and implement pooled/non-pooled versions.
 */
class DispatchClient {
  private static final StreamObserver<Worker.CancelResponse> NO_OP_CANCEL_STREAM_OBSERVER = new CancelObserver();

  private final ManagedChannel _channel;
  private final PinotQueryWorkerGrpc.PinotQueryWorkerStub _dispatchStub;

  public DispatchClient(String host, int port, @Nullable TlsConfig tlsConfig) {
    if (tlsConfig == null) {
      _channel = ManagedChannelBuilder
          .forAddress(host, port)
          .usePlaintext()
          .build();
    } else {
      _channel = NettyChannelBuilder
          .forAddress(host, port)
          .sslContext(ServerGrpcQueryClient.buildSslContext(tlsConfig))
          .build();
    }
    _dispatchStub = PinotQueryWorkerGrpc.newStub(_channel);
  }

  public ManagedChannel getChannel() {
    return _channel;
  }

  public void submit(Worker.QueryRequest request, QueryServerInstance virtualServer, Deadline deadline,
      Consumer<AsyncResponse<Worker.QueryResponse>> callback) {
    _dispatchStub.withDeadline(deadline).submit(request, new LastValueDispatchObserver<>(virtualServer, callback));
  }

  public void cancel(long requestId) {
    String cid = QueryThreadContext.isInitialized() && QueryThreadContext.getCid() != null
        ? QueryThreadContext.getCid()
        : Long.toString(requestId);
    Worker.CancelRequest cancelRequest = Worker.CancelRequest.newBuilder()
        .setRequestId(requestId)
        .setCid(cid)
        .build();
    _dispatchStub.cancel(cancelRequest, NO_OP_CANCEL_STREAM_OBSERVER);
  }

  public void explain(Worker.QueryRequest request, QueryServerInstance virtualServer, Deadline deadline,
      Consumer<AsyncResponse<List<Worker.ExplainResponse>>> callback) {
    _dispatchStub.withDeadline(deadline).explain(request, new AllValuesDispatchObserver<>(virtualServer, callback));
  }
}

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

import com.google.common.base.Preconditions;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.grpc.ServerGrpcQueryClient;
import org.apache.pinot.query.routing.QueryServerInstance;


/**
 * Dispatches a query plan to given a {@link QueryServerInstance}. Each {@link DispatchClient} has its own gRPC Channel
 * and Client Stub.
 * TODO: It might be neater to implement pooling at the client level. Two options: (1) Pass a channel provider and
 *       let that take care of pooling. (2) Create a DispatchClient interface and implement pooled/non-pooled versions.
 */
class DispatchClient {
  private static final StreamObserver<Worker.CancelResponse> NO_OP_CANCEL_STREAM_OBSERVER = new CancelObserver();
  /**
   * Shared buffer allocator configured to prefer direct (off-heap) buffers for better performance.
   * Using a static allocator allows for better memory pooling across all DispatchClient instances.
   */
  private static final PooledByteBufAllocator BUF_ALLOCATOR = new PooledByteBufAllocator(true);

  private final ManagedChannel _channel;
  private final PinotQueryWorkerGrpc.PinotQueryWorkerStub _dispatchStub;

  public DispatchClient(String host, int port, @Nullable TlsConfig tlsConfig) {
    this(host, port, tlsConfig, null, KeepAliveConfig.DISABLED);
  }

  public DispatchClient(String host, int port, @Nullable TlsConfig tlsConfig, @Nullable SslContext sslContext) {
    this(host, port, tlsConfig, sslContext, KeepAliveConfig.DISABLED);
  }

  DispatchClient(String host, int port, @Nullable TlsConfig tlsConfig, @Nullable SslContext sslContext,
      KeepAliveConfig keepAliveConfig) {
    // Always use NettyChannelBuilder to allow setting Netty-specific channel options like the buffer allocator.
    // This ensures we can explicitly configure direct (off-heap) buffers for better performance.
    NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(host, port)
        .withOption(ChannelOption.ALLOCATOR, BUF_ALLOCATOR);
    if (tlsConfig == null) {
      channelBuilder.usePlaintext();
    } else {
      SslContext contextToUse = sslContext != null ? sslContext : ServerGrpcQueryClient.buildSslContext(tlsConfig);
      channelBuilder.sslContext(contextToUse);
    }
    // Enable gRPC keep-alive when configured so that a silently unreachable peer transitions the channel out of READY,
    // which lets the broker's FailureDetector exclude it from routing.
    if (keepAliveConfig.isEnabled()) {
      channelBuilder.keepAliveTime(keepAliveConfig.getTimeMs(), TimeUnit.MILLISECONDS)
          .keepAliveTimeout(keepAliveConfig.getTimeoutMs(), TimeUnit.MILLISECONDS)
          .keepAliveWithoutCalls(keepAliveConfig.isWithoutCalls());
    }
    _channel = channelBuilder.build();
    _dispatchStub = PinotQueryWorkerGrpc.newStub(_channel);
  }

  /// Immutable gRPC keep-alive configuration for broker dispatch channels. Keep-alive is disabled when `timeMs` is not
  /// positive.
  static final class KeepAliveConfig {
    static final KeepAliveConfig DISABLED = new KeepAliveConfig(-1, 30_000, false);

    private final int _timeMs;
    private final int _timeoutMs;
    private final boolean _withoutCalls;

    KeepAliveConfig(int timeMs, int timeoutMs, boolean withoutCalls) {
      if (timeMs > 0) {
        Preconditions.checkArgument(timeoutMs > 0,
            "keepAliveTimeoutMs must be positive when keep-alive is enabled, got: %s", timeoutMs);
      }
      _timeMs = timeMs;
      _timeoutMs = timeoutMs;
      _withoutCalls = withoutCalls;
    }

    boolean isEnabled() {
      return _timeMs > 0;
    }

    int getTimeMs() {
      return _timeMs;
    }

    int getTimeoutMs() {
      return _timeoutMs;
    }

    boolean isWithoutCalls() {
      return _withoutCalls;
    }
  }

  public ManagedChannel getChannel() {
    return _channel;
  }

  public void submit(Worker.QueryRequest request, QueryServerInstance virtualServer, Deadline deadline,
      Consumer<AsyncResponse<Worker.QueryResponse>> callback) {
    _dispatchStub.withDeadline(deadline).submit(request, new LastValueDispatchObserver<>(virtualServer, callback));
  }

  public void cancelAsync(long requestId) {
    Worker.CancelRequest cancelRequest = Worker.CancelRequest.newBuilder().setRequestId(requestId).build();
    _dispatchStub.cancel(cancelRequest, NO_OP_CANCEL_STREAM_OBSERVER);
  }

  public void cancel(long requestId, QueryServerInstance virtualServer, Deadline deadline,
      Consumer<AsyncResponse<Worker.CancelResponse>> callback) {
    Worker.CancelRequest cancelRequest = Worker.CancelRequest.newBuilder().setRequestId(requestId).build();
    StreamObserver<Worker.CancelResponse> observer = new LastValueDispatchObserver<>(virtualServer, callback);
    _dispatchStub.withDeadline(deadline).cancel(cancelRequest, observer);
  }

  public void explain(Worker.QueryRequest request, QueryServerInstance virtualServer, Deadline deadline,
      Consumer<AsyncResponse<List<Worker.ExplainResponse>>> callback) {
    _dispatchStub.withDeadline(deadline).explain(request, new AllValuesDispatchObserver<>(virtualServer, callback));
  }
}

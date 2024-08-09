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
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import javax.net.ssl.SSLException;
import nl.altindag.ssl.SSLFactory;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.tls.PinotInsecureMode;
import org.apache.pinot.common.utils.tls.RenewableTlsUtils;
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
  private static final StreamObserver<Worker.CancelResponse> NO_OP_CANCEL_STREAM_OBSERVER = new CancelObserver();
  private static final Logger LOGGER = LoggerFactory.getLogger(DispatchClient.class);
  // the key is the hashCode of the TlsConfig, the value is the SslContext
  // We don't use TlsConfig as the map key because the TlsConfig is mutable, which means the hashCode can change. If the
  // hashCode changes and the map is resized, the SslContext of the old hashCode will be lost.
  private static final Map<Integer, SslContext> CLIENT_SSL_CONTEXTS_CACHE = new ConcurrentHashMap<>();

  private final ManagedChannel _channel;
  private final PinotQueryWorkerGrpc.PinotQueryWorkerStub _dispatchStub;

  public DispatchClient(String host, int port) {
    this(host, port, new GrpcConfig(Collections.emptyMap()));
  }
  public DispatchClient(String host, int port, GrpcConfig grpcConfig) {
    if (grpcConfig.isUsePlainText()) {
      _channel =
          ManagedChannelBuilder.forAddress(host, port).maxInboundMessageSize(grpcConfig.getMaxInboundMessageSizeBytes())
              .usePlaintext().build();
    } else {
      _channel =
          NettyChannelBuilder.forAddress(host, port).maxInboundMessageSize(grpcConfig.getMaxInboundMessageSizeBytes())
              .sslContext(buildSslContext(grpcConfig.getTlsConfig())).build();
    }
    _dispatchStub = PinotQueryWorkerGrpc.newStub(_channel);
  }

  public ManagedChannel getChannel() {
    return _channel;
  }

  public void submit(Worker.QueryRequest request, QueryServerInstance virtualServer, Deadline deadline,
      Consumer<AsyncQueryDispatchResponse> callback) {
    _dispatchStub.withDeadline(deadline).submit(request, new DispatchObserver(virtualServer, callback));
  }

  public void cancel(long requestId) {
    Worker.CancelRequest cancelRequest = Worker.CancelRequest.newBuilder().setRequestId(requestId).build();
    _dispatchStub.cancel(cancelRequest, NO_OP_CANCEL_STREAM_OBSERVER);
  }
  private SslContext buildSslContext(TlsConfig tlsConfig) {
    LOGGER.info("Building gRPC SSL context");
    SslContext sslContext = CLIENT_SSL_CONTEXTS_CACHE.computeIfAbsent(tlsConfig.hashCode(), tlsConfigHashCode -> {
      try {
        SSLFactory sslFactory = RenewableTlsUtils.createSSLFactoryAndEnableAutoRenewalWhenUsingFileStores(tlsConfig,
            PinotInsecureMode::isPinotInInsecureMode);
        SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
        sslFactory.getKeyManagerFactory().ifPresent(sslContextBuilder::keyManager);
        sslFactory.getTrustManagerFactory().ifPresent(sslContextBuilder::trustManager);
        if (tlsConfig.getSslProvider() != null) {
          sslContextBuilder =
              GrpcSslContexts.configure(sslContextBuilder, SslProvider.valueOf(tlsConfig.getSslProvider()));
        } else {
          sslContextBuilder = GrpcSslContexts.configure(sslContextBuilder);
        }
        return sslContextBuilder.build();
      } catch (SSLException e) {
        throw new RuntimeException("Failed to build gRPC SSL context", e);
      }
    });
    return sslContext;
  }
}

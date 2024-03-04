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
package org.apache.pinot.common.utils.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import nl.altindag.ssl.SSLFactory;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.proto.PinotQueryServerGrpc;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.tls.RenewableTlsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GrpcQueryClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcQueryClient.class);
  private static final int DEFAULT_CHANNEL_SHUTDOWN_TIMEOUT_SECOND = 10;
  // the key is the hashCode of the TlsConfig, the value is the SslContext
  // We don't use TlsConfig as the map key because the TlsConfig is mutable, which means the hashCode can change. If the
  // hashCode changes and the map is resized, the SslContext of the old hashCode will be lost.
  private static final Map<Integer, SslContext> CLIENT_SSL_CONTEXTS_CACHE = new ConcurrentHashMap<>();

  private final ManagedChannel _managedChannel;
  private final PinotQueryServerGrpc.PinotQueryServerBlockingStub _blockingStub;

  public GrpcQueryClient(String host, int port) {
    this(host, port, new GrpcConfig(Collections.emptyMap()));
  }

  public GrpcQueryClient(String host, int port, GrpcConfig config) {
    if (config.isUsePlainText()) {
      _managedChannel =
          ManagedChannelBuilder.forAddress(host, port).maxInboundMessageSize(config.getMaxInboundMessageSizeBytes())
              .usePlaintext().build();
    } else {
      _managedChannel =
          NettyChannelBuilder.forAddress(host, port).maxInboundMessageSize(config.getMaxInboundMessageSizeBytes())
              .sslContext(buildSslContext(config.getTlsConfig())).build();
    }
    _blockingStub = PinotQueryServerGrpc.newBlockingStub(_managedChannel);
  }

  private SslContext buildSslContext(TlsConfig tlsConfig) {
    LOGGER.info("Building gRPC SSL context");
    SslContext sslContext = CLIENT_SSL_CONTEXTS_CACHE.computeIfAbsent(tlsConfig.hashCode(), tlsConfigHashCode -> {
      try {
        SSLFactory sslFactory = RenewableTlsUtils.createSSLFactoryAndEnableAutoRenewalWhenUsingFileStores(tlsConfig);
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

  public Iterator<Server.ServerResponse> submit(Server.ServerRequest request) {
    return _blockingStub.submit(request);
  }

  public void close() {
    if (!_managedChannel.isShutdown()) {
      try {
        _managedChannel.shutdownNow();
        if (!_managedChannel.awaitTermination(DEFAULT_CHANNEL_SHUTDOWN_TIMEOUT_SECOND, TimeUnit.SECONDS)) {
          LOGGER.warn("Timed out forcefully shutting down connection: {}. ", _managedChannel);
        }
      } catch (Exception e) {
        LOGGER.error("Unexpected exception while waiting for channel termination", e);
      }
    }
  }
}

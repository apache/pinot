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
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.proto.PinotQueryServerGrpc;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.tls.TlsResourceBundle;
import org.apache.pinot.common.tls.TlsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GrpcQueryClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcQueryClient.class);
  private static final int DEFAULT_CHANNEL_SHUTDOWN_TIMEOUT_SECOND = 10;

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
      try {
        TlsResourceBundle tlsResourceBundle = TlsUtils.createTlsBundle(config.getTlsConfig());
        SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
        if (tlsResourceBundle.getKeyManagerFactory() != null) {
          sslContextBuilder.keyManager(tlsResourceBundle.getKeyManagerFactory());
        }
        if (tlsResourceBundle.getTrustManagerFactory() != null) {
          sslContextBuilder.trustManager(tlsResourceBundle.getTrustManagerFactory());
        }
        if (config.getTlsConfig().getSslProvider() != null) {
          sslContextBuilder =
              GrpcSslContexts.configure(sslContextBuilder, SslProvider.valueOf(config.getTlsConfig().getSslProvider()));
        } else {
          sslContextBuilder = GrpcSslContexts.configure(sslContextBuilder);
        }
        _managedChannel =
            NettyChannelBuilder.forAddress(host, port).maxInboundMessageSize(config.getMaxInboundMessageSizeBytes())
                .sslContext(sslContextBuilder.build()).build();
      } catch (SSLException e) {
        throw new RuntimeException("Failed to create Netty gRPC channel with SSL Context", e);
      }
    }
    _blockingStub = PinotQueryServerGrpc.newBlockingStub(_managedChannel);
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

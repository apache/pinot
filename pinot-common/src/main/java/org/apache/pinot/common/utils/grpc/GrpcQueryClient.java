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

import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import java.util.Iterator;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.proto.PinotQueryServerGrpc;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.spi.env.PinotConfiguration;


public class GrpcQueryClient {
  private final ManagedChannel _managedChannel;
  private final PinotQueryServerGrpc.PinotQueryServerBlockingStub _blockingStub;

  public GrpcQueryClient(String host, int port) {
    this(host, port, new Config());
  }

  public GrpcQueryClient(String host, int port, Config config) {
    if (config.isUsePlainText()) {
      _managedChannel =
          ManagedChannelBuilder.forAddress(host, port).maxInboundMessageSize(config.getMaxInboundMessageSizeBytes())
              .usePlaintext().build();
    } else {
      try {
        SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
        if (config.getTlsConfig().getKeyStorePath() != null) {
          KeyManagerFactory keyManagerFactory = TlsUtils.createKeyManagerFactory(config.getTlsConfig());
          sslContextBuilder.keyManager(keyManagerFactory);
        }
        if (config.getTlsConfig().getTrustStorePath() != null) {
          TrustManagerFactory trustManagerFactory = TlsUtils.createTrustManagerFactory(config.getTlsConfig());
          sslContextBuilder.trustManager(trustManagerFactory);
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
      _managedChannel.shutdownNow();
    }
  }

  public static class Config {
    public static final String GRPC_TLS_PREFIX = "tls";
    public static final String CONFIG_USE_PLAIN_TEXT = "usePlainText";
    public static final String CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE = "maxInboundMessageSizeBytes";
    // Default max message size to 128MB
    public static final int DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE = 128 * 1024 * 1024;

    private final int _maxInboundMessageSizeBytes;
    private final boolean _usePlainText;
    private final TlsConfig _tlsConfig;
    private final PinotConfiguration _pinotConfig;

    public Config() {
      this(DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE, true);
    }

    public Config(int maxInboundMessageSizeBytes, boolean usePlainText) {
      this(ImmutableMap.of(CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE, maxInboundMessageSizeBytes, CONFIG_USE_PLAIN_TEXT,
          usePlainText));
    }

    public Config(Map<String, Object> configMap) {
      _pinotConfig = new PinotConfiguration(configMap);
      _maxInboundMessageSizeBytes =
          _pinotConfig.getProperty(CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE, DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE);
      _usePlainText = Boolean.valueOf(configMap.get(CONFIG_USE_PLAIN_TEXT).toString());
      _tlsConfig = TlsUtils.extractTlsConfig(_pinotConfig, GRPC_TLS_PREFIX);
    }

    // Allow get customized configs.
    public Object get(String key) {
      return _pinotConfig.getProperty(key);
    }

    public int getMaxInboundMessageSizeBytes() {
      return _maxInboundMessageSizeBytes;
    }

    public boolean isUsePlainText() {
      return _usePlainText;
    }

    public TlsConfig getTlsConfig() {
      return _tlsConfig;
    }

    public PinotConfiguration getPinotConfig() {
      return _pinotConfig;
    }
  }
}

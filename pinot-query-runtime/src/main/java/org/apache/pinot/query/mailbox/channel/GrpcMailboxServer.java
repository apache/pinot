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
package org.apache.pinot.query.mailbox.channel;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;

import static org.apache.log4j.builders.appender.SocketAppenderBuilder.LOGGER;


/**
 * {@code GrpcMailboxServer} manages GRPC-based mailboxes by creating a stream-stream GRPC server.
 *
 * <p>This GRPC server is responsible for constructing {@link StreamObserver} out of an initial "open" request
 * send by the sender of the sender/receiver pair.
 */
public class GrpcMailboxServer extends PinotMailboxGrpc.PinotMailboxImplBase {
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 10_000L;

  private final MailboxService _mailboxService;
  private final Server _server;

  public GrpcMailboxServer(MailboxService mailboxService, PinotConfiguration config) {
    _mailboxService = mailboxService;
    int port = mailboxService.getPort();
    TlsConfig tlsConfig = TlsUtils.extractTlsConfig(config, CommonConstants.Server.SERVER_GRPCTLS_PREFIX);
    if(tlsConfig != null) {
      try {
        _server = NettyServerBuilder.forPort(port).addService(this).sslContext(buildGRpcSslContext(tlsConfig)).maxInboundMessageSize(
            config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES,
                CommonConstants.MultiStageQueryRunner.DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES)).build();
      } catch (Exception e) {
        throw new RuntimeException("Failed to start secure grpcMailboxServer", e);
      }
    }else {
      _server = NettyServerBuilder.forPort(port).addService(this).maxInboundMessageSize(
          config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES,
              CommonConstants.MultiStageQueryRunner.DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES)).build();
    }
  }

  public void start() {
    try {
      _server.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    try {
      _server.shutdown().awaitTermination(DEFAULT_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public StreamObserver<Mailbox.MailboxContent> open(StreamObserver<Mailbox.MailboxStatus> responseObserver) {
    return new MailboxContentObserver(_mailboxService, responseObserver);
  }
  private SslContext buildGRpcSslContext(TlsConfig tlsConfig)
      throws Exception {
    LOGGER.info("Building gRPC SSL context");
    if (tlsConfig.getKeyStorePath() == null) {
      throw new IllegalArgumentException("Must provide key store path for secured gRpc server");
    }
    SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(TlsUtils.createKeyManagerFactory(tlsConfig))
        .sslProvider(SslProvider.valueOf(tlsConfig.getSslProvider()));
    if (tlsConfig.getTrustStorePath() != null) {
      sslContextBuilder.trustManager(TlsUtils.createTrustManagerFactory(tlsConfig));
    }
    if (tlsConfig.isClientAuthEnabled()) {
      sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
    }
    return GrpcSslContexts.configure(sslContextBuilder).build();
  }
}

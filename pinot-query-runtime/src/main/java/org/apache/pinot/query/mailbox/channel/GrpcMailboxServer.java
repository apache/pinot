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

import com.google.common.base.Preconditions;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocatorMetric;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.core.transport.grpc.GrpcQueryServer;
import org.apache.pinot.query.access.AuthorizationInterceptor;
import org.apache.pinot.query.access.QueryAccessControlFactory;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


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

  public GrpcMailboxServer(MailboxService mailboxService, PinotConfiguration config, @Nullable TlsConfig tlsConfig,
      @Nullable QueryAccessControlFactory accessControlFactory) {
    _mailboxService = mailboxService;
    int port = mailboxService.getPort();
    if (accessControlFactory == null) {
      accessControlFactory = QueryAccessControlFactory.fromConfig(config);
    }

    PooledByteBufAllocator bufAllocator = new PooledByteBufAllocator(true);
    PooledByteBufAllocatorMetric metric = bufAllocator.metric();

    // Register memory metrics based on instance type
    InstanceType instanceType = mailboxService.getInstanceType();
    if (instanceType == InstanceType.BROKER) {
      BrokerMetrics brokerMetrics = BrokerMetrics.get();
      brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.MAILBOX_SERVER_USED_DIRECT_MEMORY, metric::usedDirectMemory);
      brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.MAILBOX_SERVER_USED_HEAP_MEMORY, metric::usedHeapMemory);
      brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.MAILBOX_SERVER_ARENAS_DIRECT, metric::numDirectArenas);
      brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.MAILBOX_SERVER_ARENAS_HEAP, metric::numHeapArenas);
      brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.MAILBOX_SERVER_CACHE_SIZE_SMALL, metric::smallCacheSize);
      brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.MAILBOX_SERVER_CACHE_SIZE_NORMAL, metric::normalCacheSize);
      brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.MAILBOX_SERVER_THREADLOCALCACHE, metric::numThreadLocalCaches);
      brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.MAILBOX_SERVER_CHUNK_SIZE, metric::chunkSize);

      // Notice here we are using io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent instead of
      // io.netty.util.internal.PlatformDependent because gRPC shades Netty to avoid version conflicts.
      // This also means it uses a different pool of direct memory and a different setting of max direct memory.
      //
      // Also notice these two metrics are also set by GrpcQueryService. Both are set to the same value, so it
      // doesn't matter which one _wins_ in the metrics system.
      brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.GRPC_TOTAL_MAX_DIRECT_MEMORY,
          PlatformDependent::maxDirectMemory);
      brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.GRPC_TOTAL_USED_DIRECT_MEMORY,
          PlatformDependent::usedDirectMemory);
    } else {
      Preconditions.checkState(instanceType == InstanceType.SERVER, "Unexpected instance type: %s", instanceType);
      ServerMetrics serverMetrics = ServerMetrics.get();
      serverMetrics.setOrUpdateGlobalGauge(ServerGauge.MAILBOX_SERVER_USED_DIRECT_MEMORY, metric::usedDirectMemory);
      serverMetrics.setOrUpdateGlobalGauge(ServerGauge.MAILBOX_SERVER_USED_HEAP_MEMORY, metric::usedHeapMemory);
      serverMetrics.setOrUpdateGlobalGauge(ServerGauge.MAILBOX_SERVER_ARENAS_DIRECT, metric::numDirectArenas);
      serverMetrics.setOrUpdateGlobalGauge(ServerGauge.MAILBOX_SERVER_ARENAS_HEAP, metric::numHeapArenas);
      serverMetrics.setOrUpdateGlobalGauge(ServerGauge.MAILBOX_SERVER_CACHE_SIZE_SMALL, metric::smallCacheSize);
      serverMetrics.setOrUpdateGlobalGauge(ServerGauge.MAILBOX_SERVER_CACHE_SIZE_NORMAL, metric::normalCacheSize);
      serverMetrics.setOrUpdateGlobalGauge(ServerGauge.MAILBOX_SERVER_THREADLOCALCACHE, metric::numThreadLocalCaches);
      serverMetrics.setOrUpdateGlobalGauge(ServerGauge.MAILBOX_SERVER_CHUNK_SIZE, metric::chunkSize);

      // Notice here we are using io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent instead of
      // io.netty.util.internal.PlatformDependent because gRPC shades Netty to avoid version conflicts.
      // This also means it uses a different pool of direct memory and a different setting of max direct memory.
      //
      // Also notice these two metrics are also set by GrpcQueryService. Both are set to the same value, so it
      // doesn't matter which one _wins_ in the metrics system.
      serverMetrics.setOrUpdateGlobalGauge(ServerGauge.GRPC_TOTAL_MAX_DIRECT_MEMORY,
          PlatformDependent::maxDirectMemory);
      serverMetrics.setOrUpdateGlobalGauge(ServerGauge.GRPC_TOTAL_USED_DIRECT_MEMORY,
          PlatformDependent::usedDirectMemory);
    }

    NettyServerBuilder builder = NettyServerBuilder
        .forPort(port).intercept(new MailboxServerInterceptor());
    if (accessControlFactory != null) {
      builder.intercept(new AuthorizationInterceptor(accessControlFactory));
    }
    builder
        .addService(this)
        .withOption(ChannelOption.ALLOCATOR, bufAllocator)
        .withChildOption(ChannelOption.ALLOCATOR, bufAllocator)
        .maxInboundMessageSize(config.getProperty(
            CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES,
            CommonConstants.MultiStageQueryRunner.DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES));

    // Add SSL context only if TLS is configured
    if (tlsConfig != null) {
      builder.sslContext(GrpcQueryServer.buildGrpcSslContext(tlsConfig));
    }

    _server = builder.build();
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
    String mailboxId = ChannelUtils.MAILBOX_ID_CTX_KEY.get();
    return new MailboxContentObserver(_mailboxService, mailboxId, responseObserver);
  }
}

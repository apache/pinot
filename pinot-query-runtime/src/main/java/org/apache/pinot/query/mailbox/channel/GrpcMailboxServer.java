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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocatorMetric;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketTimeoutException;
import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// `GrpcMailboxServer` manages GRPC-based mailboxes by creating a stream-stream GRPC server.
///
/// This GRPC server is responsible for constructing [StreamObserver] out of an initial "open" request
/// send by the sender of the sender/receiver pair.
public class GrpcMailboxServer extends PinotMailboxGrpc.PinotMailboxImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcMailboxServer.class);
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 10_000L;
  private static final int PROTOBUF_OVERHEAD_BYTES = 1024;

  private final MailboxService _mailboxService;
  private final Server _server;
  private final PooledByteBufAllocatorMetric _bufAllocatorMetric;
  private final int _flowControlWindowBytes;
  private final int _inboundMessageCredit;
  private final boolean _manualInboundFlowControlEnabled;
  private final int _permitKeepAliveTimeMs;
  private final boolean _permitKeepAliveWithoutCalls;
  private final int _materializedChunkSize;

  /// Constructs a gRPC-based mailbox server.
  ///
  /// @param mailboxService mailbox service providing configuration such as port and instance type
  /// @param config Pinot configuration used to initialize access control and server options
  /// @param tlsConfig optional TLS configuration; when `null`, the server is started without TLS
  /// @param sslContext optional pre-built SSL context; when non-null, this context is used instead of creating a new
  ///                   one from `tlsConfig`
  /// @param accessControlFactory optional factory for building query access control; when `null`, a factory is
  ///                             created from `config`
  public GrpcMailboxServer(MailboxService mailboxService, PinotConfiguration config, @Nullable TlsConfig tlsConfig,
      @Nullable SslContext sslContext, @Nullable QueryAccessControlFactory accessControlFactory) {
    _mailboxService = mailboxService;
    int port = mailboxService.getPort();
    if (accessControlFactory == null) {
      accessControlFactory = QueryAccessControlFactory.fromConfig(config);
    }

    PooledByteBufAllocator bufAllocator = new PooledByteBufAllocator(true);
    PooledByteBufAllocatorMetric metric = bufAllocator.metric();
    _bufAllocatorMetric = metric;

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
    _flowControlWindowBytes = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_FLOW_CONTROL_WINDOW_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_FLOW_CONTROL_WINDOW_BYTES);
    _inboundMessageCredit = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_INBOUND_MESSAGE_CREDIT,
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_INBOUND_MESSAGE_CREDIT);
    _manualInboundFlowControlEnabled = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_MANUAL_INBOUND_FLOW_CONTROL_ENABLED,
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_MANUAL_INBOUND_FLOW_CONTROL_ENABLED);
    int maxInboundMessageSize = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES);
    _materializedChunkSize = Math.max(1, maxInboundMessageSize - PROTOBUF_OVERHEAD_BYTES);
    Preconditions.checkArgument(_inboundMessageCredit > 0,
        "%s must be positive, got: %s",
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_INBOUND_MESSAGE_CREDIT,
        _inboundMessageCredit);
    // A window smaller than the largest possible single message makes the stream pathological: the sender's
    // isReady() flaps on every message, since no single message can ever fit in the available credit. Fail fast
    // at startup rather than letting it manifest as flapping back-pressure mid-query.
    Preconditions.checkArgument(_flowControlWindowBytes >= maxInboundMessageSize,
        "%s (%s) must be >= %s (%s)",
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_FLOW_CONTROL_WINDOW_BYTES, _flowControlWindowBytes,
        CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES, maxInboundMessageSize);
    // Keep-alive enforcement. A peer configured with a keep-alive time below permitKeepAliveTime has its pings
    // counted as "bad" and, past the server's strike threshold, gets GOAWAY(ENHANCE_YOUR_CALM) — which drops the
    // mailbox channel mid-query. Defaults match Netty's own so that a peer left at the default keep-alive time is
    // never punished; both sides have to be tuned down together.
    _permitKeepAliveTimeMs = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_MAILBOX_SERVER_PERMIT_KEEP_ALIVE_TIME_MS,
        CommonConstants.MultiStageQueryRunner.DEFAULT_OF_MAILBOX_SERVER_PERMIT_KEEP_ALIVE_TIME_MS);
    _permitKeepAliveWithoutCalls = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_MAILBOX_SERVER_PERMIT_KEEP_ALIVE_WITHOUT_CALLS,
        CommonConstants.MultiStageQueryRunner.DEFAULT_OF_MAILBOX_SERVER_PERMIT_KEEP_ALIVE_WITHOUT_CALLS);
    if (_permitKeepAliveTimeMs > 0) {
      builder.permitKeepAliveTime(_permitKeepAliveTimeMs, TimeUnit.MILLISECONDS);
    }
    builder
        .addService(this)
        .withOption(ChannelOption.ALLOCATOR, bufAllocator)
        .withChildOption(ChannelOption.ALLOCATOR, bufAllocator)
        .maxInboundMessageSize(maxInboundMessageSize)
        .permitKeepAliveWithoutCalls(_permitKeepAliveWithoutCalls)
        .flowControlWindow(_flowControlWindowBytes);

    // Add SSL context only if TLS is configured
    if (tlsConfig != null) {
      SslContext serverSslContext =
          sslContext != null ? sslContext : GrpcQueryServer.buildGrpcSslContext(tlsConfig);
      builder.sslContext(serverSslContext);
    }

    _server = builder.build();
  }

  @VisibleForTesting
  int getPermitKeepAliveTimeMs() {
    return _permitKeepAliveTimeMs;
  }

  @VisibleForTesting
  boolean isPermitKeepAliveWithoutCalls() {
    return _permitKeepAliveWithoutCalls;
  }

  public void start() {
    LOGGER.info("Starting GrpcMailboxServer with flowControlWindow={} bytes, inboundMessageCredit={}, "
            + "manualInboundFlowControlEnabled={}, permitKeepAliveTimeMs={}, permitKeepAliveWithoutCalls={}",
        _flowControlWindowBytes, _inboundMessageCredit, _manualInboundFlowControlEnabled, _permitKeepAliveTimeMs,
        _permitKeepAliveWithoutCalls);
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

  /// Bytes of direct (off-heap) memory currently pinned by the gRPC server
  /// allocator backing this mailbox server. This is the same allocator whose
  /// values are exported as `MAILBOX_SERVER_USED_DIRECT_MEMORY` gauges.
  public long usedDirectMemoryBytes() {
    return _bufAllocatorMetric.usedDirectMemory();
  }

  /// Bytes of heap memory currently pinned by the gRPC server allocator backing
  /// this mailbox server. Exported as `MAILBOX_SERVER_USED_HEAP_MEMORY` gauges.
  public long usedHeapMemoryBytes() {
    return _bufAllocatorMetric.usedHeapMemory();
  }

  @Override
  public StreamObserver<Mailbox.MailboxContent> open(StreamObserver<Mailbox.MailboxStatus> responseObserver) {
    String mailboxId = ChannelUtils.MAILBOX_ID_CTX_KEY.get();
    ServerCallStreamObserver<Mailbox.MailboxStatus> serverCallObserver =
        (ServerCallStreamObserver<Mailbox.MailboxStatus>) responseObserver;
    if (_manualInboundFlowControlEnabled) {
      // Manual inbound flow control: override gRPC's auto-inbound (which calls request(1) after each
      // onNext) and prefetch _inboundMessageCredit messages up-front. MailboxContentObserver.onNext will
      // then replenish one credit at the top of each call so the in-flight window stays full while the
      // application drains. This is the primary throughput knob for small/medium MSE blocks.
      serverCallObserver.disableAutoInboundFlowControl();
      serverCallObserver.request(_inboundMessageCredit);
    }
    // Else: leave gRPC's auto-inbound in place — only 1 message in flight at a time. This is the pre-PR
    // behaviour, retained as a rollback knob via KEY_OF_GRPC_MANUAL_INBOUND_FLOW_CONTROL_ENABLED.
    return new MailboxContentObserver(_mailboxService, mailboxId, serverCallObserver,
        _manualInboundFlowControlEnabled);
  }

  @Override
  public void readMaterializedPartition(Mailbox.MaterializedPartitionRequest request,
      StreamObserver<Mailbox.MaterializedPartitionContent> responseObserver) {
    ServerCallStreamObserver<Mailbox.MaterializedPartitionContent> serverObserver =
        (ServerCallStreamObserver<Mailbox.MaterializedPartitionContent>) responseObserver;
    Iterator<byte[]> records;
    try {
      records = _mailboxService.readMaterializedPartitionRecords(
          request.getRequestId(), request.getProducerStageId(), request.getProducerWorkerId(),
          request.getLogicalPartitionId(), request.getDeadlineMs());
    } catch (RuntimeException e) {
      responseObserver.onError(toStatus(e));
      return;
    }
    new MaterializedPartitionPump(records, serverObserver, _materializedChunkSize).start();
  }

  private static RuntimeException toStatus(RuntimeException e) {
    if (e instanceof CancellationException) {
      return Status.CANCELLED.withDescription(e.getMessage()).withCause(e).asRuntimeException();
    }
    if (e instanceof UncheckedIOException && e.getCause() instanceof SocketTimeoutException) {
      return Status.DEADLINE_EXCEEDED.withDescription(e.getMessage()).withCause(e).asRuntimeException();
    }
    return Status.INTERNAL.withDescription("Failed to read materialized partition").withCause(e).asRuntimeException();
  }

  @VisibleForTesting
  static final class MaterializedPartitionPump implements Runnable {
    private final Iterator<byte[]> _records;
    private final ServerCallStreamObserver<Mailbox.MaterializedPartitionContent> _observer;
    private final int _chunkSize;
    private final AtomicBoolean _draining = new AtomicBoolean();
    private final AtomicBoolean _terminated = new AtomicBoolean();
    private final Object _lock = new Object();

    private byte[] _record;
    private int _offset;

    MaterializedPartitionPump(Iterator<byte[]> records,
        ServerCallStreamObserver<Mailbox.MaterializedPartitionContent> observer, int chunkSize) {
      _records = records;
      _observer = observer;
      _chunkSize = chunkSize;
    }

    void start() {
      try {
        _observer.setOnCancelHandler(this::cancel);
        _observer.setOnReadyHandler(this);
        if (_observer.isCancelled()) {
          cancel();
        } else {
          run();
        }
      } catch (RuntimeException e) {
        synchronized (_lock) {
          failLocked(e);
        }
      }
    }

    @Override
    public void run() {
      if (!_draining.compareAndSet(false, true)) {
        return;
      }
      do {
        drain();
        _draining.set(false);
      } while (shouldDrain() && _draining.compareAndSet(false, true));
    }

    private void drain() {
      while (_observer.isReady()) {
        synchronized (_lock) {
          if (_terminated.get()) {
            return;
          }
          if (_observer.isCancelled()) {
            cancelLocked();
            return;
          }
          try {
            if (_record == null && !_records.hasNext()) {
              _terminated.set(true);
              _observer.onCompleted();
              return;
            }
            if (_record == null) {
              _record = _records.next();
              _offset = 0;
            }
            int length = Math.min(_chunkSize, _record.length - _offset);
            _observer.onNext(Mailbox.MaterializedPartitionContent.newBuilder()
                .setPayload(com.google.protobuf.ByteString.copyFrom(_record, _offset, length))
                .setEndOfBlock(_offset + length == _record.length)
                .build());
            _offset += length;
            if (_offset == _record.length) {
              _record = null;
            }
          } catch (RuntimeException e) {
            failLocked(e);
            return;
          }
        }
      }
    }

    private boolean shouldDrain() {
      return !_terminated.get() && !_observer.isCancelled() && _observer.isReady();
    }

    private void cancel() {
      synchronized (_lock) {
        cancelLocked();
      }
    }

    private void cancelLocked() {
      if (_terminated.compareAndSet(false, true)) {
        closeRecords();
      }
    }

    private void failLocked(RuntimeException e) {
      if (_terminated.compareAndSet(false, true)) {
        closeRecords();
        _observer.onError(toStatus(e));
      }
    }

    private void closeRecords() {
      if (_records instanceof AutoCloseable) {
        try {
          ((AutoCloseable) _records).close();
        } catch (Exception e) {
          LOGGER.debug("Failed to close materialized partition reader", e);
        }
      }
    }
  }
}

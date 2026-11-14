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
package org.apache.pinot.query.mailbox;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.MseMeter;
import org.apache.pinot.common.metrics.MseMetrics;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.grpc.ServerGrpcQueryClient;
import org.apache.pinot.core.instance.context.BrokerContext;
import org.apache.pinot.core.instance.context.ControllerContext;
import org.apache.pinot.core.instance.context.ServerContext;
import org.apache.pinot.core.transport.grpc.GrpcQueryServer;
import org.apache.pinot.query.access.QueryAccessControlFactory;
import org.apache.pinot.query.grpc.GrpcKeepAliveConfig;
import org.apache.pinot.query.grpc.ReceiverIdentity;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.apache.pinot.query.mailbox.channel.GrpcMailboxServer;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Mailbox service that handles data transfer.
public class MailboxService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxService.class);
  private static final int DANGLING_RECEIVING_MAILBOX_EXPIRY_SECONDS = 300;

  /// Cached receiving mailboxes that contains the received blocks queue.
  ///
  /// We use a cache to ensure the receiving mailbox are not leaked in the cases where the corresponding OpChain is
  /// either never registered or died before the sender finished sending data.
  private final Cache<String, ReceivingMailbox> _receivingMailboxCache =
      CacheBuilder.newBuilder().expireAfterAccess(DANGLING_RECEIVING_MAILBOX_EXPIRY_SECONDS, TimeUnit.SECONDS)
          .removalListener((RemovalListener<String, ReceivingMailbox>) notification -> {
            if (notification.wasEvicted()) {
              ReceivingMailbox receivingMailbox = notification.getValue();
              int numPendingBlocks = receivingMailbox.getNumPendingBlocks();
              if (numPendingBlocks > 0) {
                LOGGER.warn("Evicting dangling receiving mailbox: {} with {} pending blocks", notification.getKey(),
                    numPendingBlocks);
              }
              // In case there is a leak, we should cancel the mailbox to unblock any waiters and release resources.
              receivingMailbox.cancel();
            }
          }).build();

  private final String _hostname;
  private final int _port;
  private final InstanceType _instanceType;
  private final PinotConfiguration _config;
  private final ChannelManager _channelManager;
  @Nullable private final TlsConfig _tlsConfig;
  @Nullable
  private final SslContext _clientSslContext;
  @Nullable
  private final SslContext _serverSslContext;
  @Nullable private final QueryAccessControlFactory _accessControlFactory;
  /// The max inbound message size for the gRPC server.
  ///
  /// If we try to send a message larger than this value, the gRPC server will throw an exception and close the
  /// connection.
  ///
  /// The [GrpcSendingMailbox] will split the data block into smaller chunks to fit into this limit, but a very
  /// small limit (lower than hundred of KBs) will cause performance degradation and may even fail, given that some
  /// extra bloating is added by gRPC and protobuf.
  private final int _maxInboundMessageSize;
  private final boolean _grpcSenderBackpressureEnabled;
  /// Identity the mailbox server checks incoming streams against, or `null` if the instance id is unknown or the check
  /// is disabled. See [CommonConstants.MultiStageQueryRunner#KEY_OF_REJECT_MISROUTED_REQUESTS].
  @Nullable
  private final ReceiverIdentity _receiverIdentity;

  private GrpcMailboxServer _grpcMailboxServer;

  public MailboxService(String hostname, int port, InstanceType instanceType, PinotConfiguration config) {
    this(hostname, port, instanceType, config, null);
  }

  public MailboxService(String hostname, int port, InstanceType instanceType, PinotConfiguration config,
      @Nullable TlsConfig tlsConfig) {
    this(hostname, port, instanceType, config, tlsConfig, null);
  }

  public MailboxService(String hostname, int port, InstanceType instanceType, PinotConfiguration config,
      @Nullable TlsConfig tlsConfig, @Nullable QueryAccessControlFactory accessControlFactory) {
    this(hostname, port, instanceType, config, tlsConfig, accessControlFactory, null);
  }

  /// @param instanceId the id of the instance this service belongs to, or `null` if unknown. Senders that know which
  ///                   instance they mean say so in each stream, and the mailbox server rejects a stream meant for
  ///                   another instance, see [ReceiverIdentity]. Without an id, it accepts every stream.
  public MailboxService(String hostname, int port, InstanceType instanceType, PinotConfiguration config,
      @Nullable TlsConfig tlsConfig, @Nullable QueryAccessControlFactory accessControlFactory,
      @Nullable String instanceId) {
    _hostname = hostname;
    _port = port;
    _instanceType = instanceType;
    _config = config;
    _tlsConfig = tlsConfig;
    _clientSslContext = resolveClientSslContext(instanceType, tlsConfig);
    _serverSslContext = resolveServerSslContext(instanceType, tlsConfig);
    _maxInboundMessageSize = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES
    );
    _grpcSenderBackpressureEnabled = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_SENDER_BACKPRESSURE_ENABLED,
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_SENDER_BACKPRESSURE_ENABLED
    );
    int writeBufferHighWaterMarkBytes = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_WRITE_BUFFER_HIGH_WATER_MARK_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_HIGH_WATER_MARK_BYTES);
    int writeBufferLowWaterMarkBytes = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_WRITE_BUFFER_LOW_WATER_MARK_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_LOW_WATER_MARK_BYTES);
    GrpcKeepAliveConfig keepAliveConfig = GrpcKeepAliveConfig.forMailboxChannels(config);
    _channelManager = new ChannelManager(_clientSslContext, _maxInboundMessageSize, getIdleTimeout(config),
        writeBufferHighWaterMarkBytes, writeBufferLowWaterMarkBytes, keepAliveConfig);
    _accessControlFactory = accessControlFactory;
    boolean rejectMisroutedRequests = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_REJECT_MISROUTED_REQUESTS,
        CommonConstants.MultiStageQueryRunner.DEFAULT_REJECT_MISROUTED_REQUESTS);
    _receiverIdentity = instanceId != null && rejectMisroutedRequests
        ? new ReceiverIdentity(instanceId, hostname + ":" + port) : null;
    registerMailboxClientGauges();
    LOGGER.info("Initialized MailboxService with hostname: {}, port: {}, channel {}", hostname, port,
        keepAliveConfig);
  }

  /// Registers gauges exposing the memory used by the gRPC client allocator
  /// shared by every [GrpcSendingMailbox] this service creates. The companion
  /// gauges for the server allocator are registered in [GrpcMailboxServer].
  ///
  /// Notice we are wiring the shaded gRPC Netty allocator
  /// ([io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator]) rather than
  /// the non-shaded one.
  ///
  /// Only the two `MAILBOX_CLIENT_USED_*` gauges are mirrored on the client side;
  /// the six debug-only counterparts exposed by the server (`MAILBOX_SERVER_ARENAS_*`,
  /// `MAILBOX_SERVER_CACHE_SIZE_*`, `MAILBOX_SERVER_THREADLOCALCACHE`,
  /// `MAILBOX_SERVER_CHUNK_SIZE`) are intentionally not mirrored here. The operational
  /// signal that matters on the client is direct-memory pool exhaustion (the OOM that
  /// motivated wiring these gauges in the first place); the rest are server-side
  /// allocator-internals useful only for debugging. If you ever need to mirror the
  /// remaining six, copy the registration block from [GrpcMailboxServer]'s constructor.
  private void registerMailboxClientGauges() {
    switch (_instanceType) {
      case BROKER: {
        BrokerMetrics brokerMetrics = BrokerMetrics.get();
        brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.MAILBOX_CLIENT_USED_DIRECT_MEMORY,
            _channelManager::usedDirectMemoryBytes);
        brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.MAILBOX_CLIENT_USED_HEAP_MEMORY,
            _channelManager::usedHeapMemoryBytes);
        break;
      }
      case SERVER: {
        ServerMetrics serverMetrics = ServerMetrics.get();
        serverMetrics.setOrUpdateGlobalGauge(ServerGauge.MAILBOX_CLIENT_USED_DIRECT_MEMORY,
            _channelManager::usedDirectMemoryBytes);
        serverMetrics.setOrUpdateGlobalGauge(ServerGauge.MAILBOX_CLIENT_USED_HEAP_MEMORY,
            _channelManager::usedHeapMemoryBytes);
        break;
      }
      default:
        // Controller does not run a MailboxService in production today, but if one is ever
        // started for tests we silently skip metric registration rather than failing.
        break;
    }
  }

  /// Starts the mailbox service.
  public void start() {
    LOGGER.info("Starting GrpcMailboxServer");
    _grpcMailboxServer = new GrpcMailboxServer(this, _config, _tlsConfig, _serverSslContext, _accessControlFactory);
    _grpcMailboxServer.start();
  }

  /// Shuts down the mailbox service.
  public void shutdown() {
    LOGGER.info("Shutting down GrpcMailboxServer");
    _grpcMailboxServer.shutdown();
    _channelManager.shutdown();
  }

  public String getHostname() {
    return _hostname;
  }

  public int getPort() {
    return _port;
  }

  public InstanceType getInstanceType() {
    return _instanceType;
  }

  /// Returns the identity the mailbox server checks incoming streams against, or `null` if there is no check.
  @Nullable
  public ReceiverIdentity getReceiverIdentity() {
    return _receiverIdentity;
  }

  /// Returns a sending mailbox for the given mailbox id. The returned sending mailbox is uninitialized, i.e. it will
  /// not open the underlying channel or acquire any additional resources. Instead, it will initialize lazily when the
  /// data is sent for the first time.
  // TODO(https://github.com/apache/pinot/issues/18539): add a global byte budget across peers; the
  //  per-channel WriteBufferWaterMark bound is watermark.high × #peers × #concurrent_queries, which can
  //  reach multiple GiB and re-trigger OutOfDirectMemoryError at large fan-outs.
  public SendingMailbox getSendingMailbox(String hostname, int port, String mailboxId, long deadlineMs,
      StatMap<MailboxSendOperator.StatKey> statMap) {
    return getSendingMailbox(hostname, port, 0, mailboxId, deadlineMs, statMap);
  }

  /// Same as [#getSendingMailbox(String, int, String, long, StatMap)], for a receiver whose instance id hash is known
  /// (0 if unknown). Another instance that the stream reaches through a stale connection rejects it, which makes the
  /// sending mailbox reset the connection and fail. See [ReceiverIdentity].
  public SendingMailbox getSendingMailbox(String hostname, int port, long receiverInstanceIdHash, String mailboxId,
      long deadlineMs, StatMap<MailboxSendOperator.StatKey> statMap) {
    if (_hostname.equals(hostname) && _port == port) {
      return new InMemorySendingMailbox(mailboxId, this, deadlineMs, statMap);
    } else {
      return new GrpcSendingMailbox(mailboxId, _channelManager, hostname, port, receiverInstanceIdHash, deadlineMs,
          statMap, _maxInboundMessageSize, _grpcSenderBackpressureEnabled, this::recordMisroutedRequestRejectedByPeer);
    }
  }

  /// Returns the receiving mailbox for the given mailbox id.
  public ReceivingMailbox getReceivingMailbox(String mailboxId) {
    try {
      return _receivingMailboxCache.get(mailboxId, () -> new ReceivingMailbox(mailboxId));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /// Makes the channel to the given server resolve its hostname again before the next send.
  /// @see ChannelManager#refreshChannel(String, int)
  /// @return false if there is no channel to the server, or it was just refreshed
  public boolean refreshChannel(String hostname, int port) {
    return _channelManager.refreshChannel(hostname, port);
  }

  /// Shuts down the channel to an address that is no longer used.
  /// @see ChannelManager#retireChannel(String, int)
  /// @return false if there is no channel to the address
  public boolean retireChannel(String hostname, int port) {
    return _channelManager.retireChannel(hostname, port);
  }

  /// Counts a stream this service's mailbox server rejected as meant for another instance.
  public void recordMisroutedRequestRejected() {
    meter(MseMeter.MISROUTED_REQUESTS_REJECTED, BrokerMeter.MULTI_STAGE_MISROUTED_REQUESTS_REJECTED);
  }

  /// Counts a stream this service sent that the receiving instance rejected as meant for another instance.
  private void recordMisroutedRequestRejectedByPeer() {
    meter(MseMeter.MISROUTED_REQUESTS_REJECTED_BY_PEER, BrokerMeter.MULTI_STAGE_MISROUTED_REQUESTS_REJECTED_BY_PEER);
  }

  private void meter(MseMeter serverMeter, BrokerMeter brokerMeter) {
    switch (_instanceType) {
      case BROKER:
        BrokerMetrics.get().addMeteredGlobalValue(brokerMeter, 1L);
        break;
      case SERVER:
        MseMetrics.get().addMeteredGlobalValue(serverMeter, 1L);
        break;
      default:
        break;
    }
  }

  /// Current value of [BrokerGauge#MAILBOX_CLIENT_USED_DIRECT_MEMORY] /
  /// [ServerGauge#MAILBOX_CLIENT_USED_DIRECT_MEMORY] — bytes pinned by the
  /// shared gRPC client allocator backing every [GrpcSendingMailbox] created
  /// from this service.
  /// The channel manager these mailboxes send through. Exposed so a test can assert that the policy
  /// resolved from config actually reaches the transport, which no assertion on the parsed values can
  /// show.
  @VisibleForTesting
  ChannelManager getChannelManager() {
    return _channelManager;
  }

  @VisibleForTesting
  public long getMailboxClientUsedDirectMemoryBytes() {
    return _channelManager.usedDirectMemoryBytes();
  }

  /// Current value of [BrokerGauge#MAILBOX_CLIENT_USED_HEAP_MEMORY] /
  /// [ServerGauge#MAILBOX_CLIENT_USED_HEAP_MEMORY].
  @VisibleForTesting
  public long getMailboxClientUsedHeapMemoryBytes() {
    return _channelManager.usedHeapMemoryBytes();
  }

  /// Current value of [BrokerGauge#MAILBOX_SERVER_USED_DIRECT_MEMORY] /
  /// [ServerGauge#MAILBOX_SERVER_USED_DIRECT_MEMORY] — bytes pinned by the
  /// gRPC server allocator handling inbound mailbox traffic.
  ///
  /// Returns 0 before [#start] is called (the gRPC server is built lazily there).
  @VisibleForTesting
  public long getMailboxServerUsedDirectMemoryBytes() {
    return _grpcMailboxServer != null ? _grpcMailboxServer.usedDirectMemoryBytes() : 0L;
  }

  /// Current value of [BrokerGauge#MAILBOX_SERVER_USED_HEAP_MEMORY] /
  /// [ServerGauge#MAILBOX_SERVER_USED_HEAP_MEMORY].
  ///
  /// Returns 0 before [#start] is called.
  @VisibleForTesting
  public long getMailboxServerUsedHeapMemoryBytes() {
    return _grpcMailboxServer != null ? _grpcMailboxServer.usedHeapMemoryBytes() : 0L;
  }

  /// Returns the receiving mailbox with the given id if it exists, without creating it.
  @VisibleForTesting
  @Nullable
  ReceivingMailbox getReceivingMailboxIfPresent(String mailboxId) {
    return _receivingMailboxCache.getIfPresent(mailboxId);
  }

  /// Releases the receiving mailbox from the cache.
  ///
  /// The receiving mailbox for a given OpChain may be created before the OpChain is even registered. Reason being that
  /// the sender starts sending data, and the receiver starts receiving the same without waiting for the OpChain to be
  /// registered. The ownership for the ReceivingMailbox hence lies with the MailboxService and not the OpChain.
  ///
  /// We can safely release a receiving mailbox when all the data are received and processed by the OpChain. If there
  /// might be data not received yet, we should not release the receiving mailbox to prevent a new receiving mailbox
  /// being created.
  public void releaseReceivingMailbox(ReceivingMailbox mailbox) {
    _receivingMailboxCache.invalidate(mailbox.getId());
  }

  private static Duration getIdleTimeout(PinotConfiguration config) {
    long channelIdleTimeoutSeconds = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_CHANNEL_IDLE_TIMEOUT_SECONDS,
        CommonConstants.MultiStageQueryRunner.DEFAULT_CHANNEL_IDLE_TIMEOUT_SECONDS);
    if (channelIdleTimeoutSeconds > 0) {
      return Duration.ofSeconds(channelIdleTimeoutSeconds);
    }
    // Use a reasonable maximum idle timeout (1 year) to avoid overflow.
    return Duration.ofDays(365);
  }

  @Nullable
  private static SslContext resolveClientSslContext(InstanceType instanceType, @Nullable TlsConfig tlsConfig) {
    if (tlsConfig == null) {
      return null;
    }
    return getOrBuildClientSslContext(instanceType, tlsConfig);
  }

  @Nullable
  private static SslContext resolveServerSslContext(InstanceType instanceType, @Nullable TlsConfig tlsConfig) {
    if (tlsConfig == null) {
      return null;
    }
    return getOrBuildServerSslContext(instanceType, tlsConfig);
  }

  private static SslContext getOrBuildClientSslContext(InstanceType instanceType, TlsConfig tlsConfig) {
    return getOrBuildSslContext(instanceType, tlsConfig, ServerGrpcQueryClient::buildSslContext,
        GrpcSslContextAccessor::getClientGrpcSslContext, GrpcSslContextAccessor::setClientGrpcSslContext);
  }

  private static SslContext getOrBuildServerSslContext(InstanceType instanceType, TlsConfig tlsConfig) {
    return getOrBuildSslContext(instanceType, tlsConfig, GrpcQueryServer::buildGrpcSslContext,
        GrpcSslContextAccessor::getServerGrpcSslContext, GrpcSslContextAccessor::setServerGrpcSslContext);
  }

  private static SslContext getOrBuildSslContext(InstanceType instanceType, TlsConfig tlsConfig,
      Function<TlsConfig, SslContext> builder, Function<GrpcSslContextAccessor, SslContext> getter,
      BiConsumer<GrpcSslContextAccessor, SslContext> setter) {
    GrpcSslContextAccessor accessor = getGrpcSslContextAccessor(instanceType);
    if (accessor == null) {
      return builder.apply(tlsConfig);
    }
    SslContext sslContext = getter.apply(accessor);
    if (sslContext == null) {
      sslContext = builder.apply(tlsConfig);
      setter.accept(accessor, sslContext);
    }
    return sslContext;
  }

  @Nullable
  private static GrpcSslContextAccessor getGrpcSslContextAccessor(InstanceType instanceType) {
    switch (instanceType) {
      case BROKER:
        return BROKER_GRPC_CONTEXT_ACCESSOR;
      case SERVER:
        return SERVER_GRPC_CONTEXT_ACCESSOR;
      case CONTROLLER:
        return CONTROLLER_GRPC_CONTEXT_ACCESSOR;
      default:
        return null;
    }
  }

  private interface GrpcSslContextAccessor {
    @Nullable
    SslContext getClientGrpcSslContext();

    void setClientGrpcSslContext(SslContext sslContext);

    @Nullable
    SslContext getServerGrpcSslContext();

    void setServerGrpcSslContext(SslContext sslContext);
  }

  private static final GrpcSslContextAccessor BROKER_GRPC_CONTEXT_ACCESSOR = new GrpcSslContextAccessor() {
    @Override
    public SslContext getClientGrpcSslContext() {
      return BrokerContext.getInstance().getClientGrpcSslContext();
    }

    @Override
    public void setClientGrpcSslContext(SslContext sslContext) {
      BrokerContext.getInstance().setClientGrpcSslContext(sslContext);
    }

    @Override
    public SslContext getServerGrpcSslContext() {
      return BrokerContext.getInstance().getServerGrpcSslContext();
    }

    @Override
    public void setServerGrpcSslContext(SslContext sslContext) {
      BrokerContext.getInstance().setServerGrpcSslContext(sslContext);
    }
  };

  private static final GrpcSslContextAccessor SERVER_GRPC_CONTEXT_ACCESSOR = new GrpcSslContextAccessor() {
    @Override
    public SslContext getClientGrpcSslContext() {
      return ServerContext.getInstance().getClientGrpcSslContext();
    }

    @Override
    public void setClientGrpcSslContext(SslContext sslContext) {
      ServerContext.getInstance().setClientGrpcSslContext(sslContext);
    }

    @Override
    public SslContext getServerGrpcSslContext() {
      return ServerContext.getInstance().getServerGrpcSslContext();
    }

    @Override
    public void setServerGrpcSslContext(SslContext sslContext) {
      ServerContext.getInstance().setServerGrpcSslContext(sslContext);
    }
  };

  private static final GrpcSslContextAccessor CONTROLLER_GRPC_CONTEXT_ACCESSOR = new GrpcSslContextAccessor() {
    @Override
    public SslContext getClientGrpcSslContext() {
      return ControllerContext.getInstance().getClientGrpcSslContext();
    }

    @Override
    public void setClientGrpcSslContext(SslContext sslContext) {
      ControllerContext.getInstance().setClientGrpcSslContext(sslContext);
    }

    @Override
    public SslContext getServerGrpcSslContext() {
      return ControllerContext.getInstance().getServerGrpcSslContext();
    }

    @Override
    public void setServerGrpcSslContext(SslContext sslContext) {
      ControllerContext.getInstance().setServerGrpcSslContext(sslContext);
    }
  };
}

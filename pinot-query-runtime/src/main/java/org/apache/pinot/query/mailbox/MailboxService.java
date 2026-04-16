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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.grpc.ServerGrpcQueryClient;
import org.apache.pinot.core.instance.context.BrokerContext;
import org.apache.pinot.core.instance.context.ControllerContext;
import org.apache.pinot.core.instance.context.ServerContext;
import org.apache.pinot.core.transport.grpc.GrpcQueryServer;
import org.apache.pinot.query.access.QueryAccessControlFactory;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.apache.pinot.query.mailbox.channel.GrpcMailboxServer;
import org.apache.pinot.query.mailbox.flight.FlightChannelManager;
import org.apache.pinot.query.mailbox.flight.FlightMailboxServer;
import org.apache.pinot.query.mailbox.flight.FlightSendingMailbox;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.segment.spi.memory.ArrowBuffers;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mailbox service that handles data transfer.
 */
public class MailboxService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxService.class);
  private static final int DANGLING_RECEIVING_MAILBOX_EXPIRY_SECONDS = 300;

  /**
   * Cached receiving mailboxes that contains the received blocks queue.
   *
   * We use a cache to ensure the receiving mailbox are not leaked in the cases where the corresponding OpChain is
   * either never registered or died before the sender finished sending data.
   */
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
  private final ArrowBuffers _arrowBuffers;
  private final int _flightPort;
  @Nullable
  private FlightChannelManager _flightChannelManager;
  @Nullable
  private FlightMailboxServer _flightServer;
  /**
   * The max inbound message size for the gRPC server.
   *
   * If we try to send a message larger than this value, the gRPC server will throw an exception and close the
   * connection.
   *
   * The {@link GrpcSendingMailbox} will split the data block into smaller chunks to fit into this limit, but a very
   * small limit (lower than hundred of KBs) will cause performance degradation and may even fail, given that some extra
   * bloating is added by gRPC and protobuf.
   */
  private final int _maxInboundMessageSize;

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
    this(hostname, port, instanceType, config, tlsConfig, accessControlFactory,
        ArrowBuffers.create(config));
  }

  public MailboxService(String hostname, int port, InstanceType instanceType, PinotConfiguration config,
      @Nullable TlsConfig tlsConfig, @Nullable QueryAccessControlFactory accessControlFactory,
      ArrowBuffers arrowBuffers) {
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
    _channelManager = new ChannelManager(_clientSslContext, _maxInboundMessageSize, getIdleTimeout(config));
    _accessControlFactory = accessControlFactory;
    _arrowBuffers = arrowBuffers;
    // Derive Flight port: explicit config, or gRPC port + 1 when Arrow is enabled
    int configuredFlightPort = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_FLIGHT_PORT,
        CommonConstants.MultiStageQueryRunner.DEFAULT_QUERY_RUNNER_FLIGHT_PORT);
    _flightPort = arrowBuffers.isEnabled() && configuredFlightPort == 0 ? port + 1 : configuredFlightPort;
    if (arrowBuffers.isEnabled()) {
      _flightChannelManager = new FlightChannelManager(tlsConfig, arrowBuffers);
    }
    LOGGER.info("Initialized MailboxService with hostname: {}, port: {}, flightPort: {}",
        hostname, port, _flightPort);
  }

  /**
   * Starts the mailbox service.
   */
  public void start() {
    LOGGER.info("Starting GrpcMailboxServer");
    _grpcMailboxServer = new GrpcMailboxServer(this, _config, _tlsConfig, _serverSslContext, _accessControlFactory);
    _grpcMailboxServer.start();
    if (_arrowBuffers.isEnabled() && _flightPort > 0) {
      LOGGER.info("Starting FlightMailboxServer on port {}", _flightPort);
      _flightServer = new FlightMailboxServer(_hostname, _flightPort, _tlsConfig, _arrowBuffers, this);
      _flightServer.start();
    }
  }

  /**
   * Shuts down the mailbox service.
   */
  public void shutdown() {
    LOGGER.info("Shutting down GrpcMailboxServer");
    _grpcMailboxServer.shutdown();
    if (_flightServer != null) {
      LOGGER.info("Shutting down FlightMailboxServer");
      _flightServer.close();
      _flightServer = null;
    }
    if (_flightChannelManager != null) {
      _flightChannelManager.close();
      _flightChannelManager = null;
    }
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

  /** Returns the {@link ArrowBuffers} instance used by this mailbox service. */
  public ArrowBuffers getArrowBuffers() {
    return _arrowBuffers;
  }

  /**
   * Creates a new per-query Arrow {@link BufferAllocator} child for the given query/stage.
   * Returns {@code null} when Arrow is disabled.
   */
  @Nullable
  public BufferAllocator newQueryArrowAllocator(String name) {
    return _arrowBuffers.isEnabled() ? _arrowBuffers.newQueryAllocator(name) : null;
  }

  /**
   * Returns a sending mailbox for the given mailbox id. The returned sending mailbox is uninitialized, i.e. it will
   * not open the underlying channel or acquire any additional resources. Instead, it will initialize lazily when the
   * data is sent for the first time.
   */
  public SendingMailbox getSendingMailbox(String hostname, int port, String mailboxId, long deadlineMs,
      StatMap<MailboxSendOperator.StatKey> statMap) {
    if (_hostname.equals(hostname) && _port == port) {
      return new InMemorySendingMailbox(mailboxId, this, deadlineMs, statMap);
    }
    // When Arrow is enabled, send data via Flight (zero-copy) with gRPC fallback for EOS/errors
    if (_flightChannelManager != null && _flightPort > 0) {
      GrpcSendingMailbox grpcFallback = new GrpcSendingMailbox(mailboxId, _channelManager, hostname, port,
          deadlineMs, statMap, _maxInboundMessageSize);
      int targetFlightPort = port + 1; // Convention: flight port = gRPC port + 1
      BufferAllocator allocator = _arrowBuffers.newQueryAllocator("flight-send");
      return new FlightSendingMailbox(mailboxId, _flightChannelManager, hostname, targetFlightPort,
          deadlineMs, statMap, grpcFallback, allocator);
    }
    return new GrpcSendingMailbox(mailboxId, _channelManager, hostname, port, deadlineMs, statMap,
        _maxInboundMessageSize);
  }

  /**
   * Returns the receiving mailbox for the given mailbox id.
   */
  public ReceivingMailbox getReceivingMailbox(String mailboxId) {
    try {
      return _receivingMailboxCache.get(mailboxId, () -> new ReceivingMailbox(mailboxId));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Resets the GRPC connection backoff for the channel to the given server.
   * @see ChannelManager#resetConnectBackoff(String, int)
   * @return true if the channel was in TRANSIENT_FAILURE and backoff was reset
   */
  public boolean resetConnectBackoff(String hostname, int port) {
    return _channelManager.resetConnectBackoff(hostname, port);
  }

  /**
   * Releases the receiving mailbox from the cache.
   *
   * The receiving mailbox for a given OpChain may be created before the OpChain is even registered. Reason being that
   * the sender starts sending data, and the receiver starts receiving the same without waiting for the OpChain to be
   * registered. The ownership for the ReceivingMailbox hence lies with the MailboxService and not the OpChain.
   *
   * We can safely release a receiving mailbox when all the data are received and processed by the OpChain. If there
   * might be data not received yet, we should not release the receiving mailbox to prevent a new receiving mailbox
   * being created.
   */
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

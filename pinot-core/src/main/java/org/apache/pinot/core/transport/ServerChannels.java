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
package org.apache.pinot.core.transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.util.OsCheck;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code ServerChannels} class manages the channels between broker to all the connected servers.
 * <p>There is only one channel between the broker and each connected server (we count OFFLINE and REALTIME as different
 * servers)
 */
@ThreadSafe
public class ServerChannels {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerChannels.class);
  public static final String CHANNEL_LOCK_TIMEOUT_MSG = "Timeout while acquiring channel lock";
  private static final long TRY_CONNECT_CHANNEL_LOCK_TIMEOUT_MS = 5_000L;

  private final QueryRouter _queryRouter;
  private final BrokerMetrics _brokerMetrics;
  // TSerializer currently is not thread safe, must be put into a ThreadLocal.
  private final ThreadLocal<TSerializer> _threadLocalTSerializer;
  private final ConcurrentHashMap<ServerRoutingInstance, ServerChannel> _serverToChannelMap = new ConcurrentHashMap<>();
  private final TlsConfig _tlsConfig;
  private final EventLoopGroup _eventLoopGroup;
  private final Class<? extends SocketChannel> _channelClass;

  /**
   * Create a server channel with TLS config
   *
   * @param queryRouter query router
   * @param brokerMetrics broker metrics
   * @param tlsConfig TLS/SSL config
   */
  public ServerChannels(QueryRouter queryRouter, BrokerMetrics brokerMetrics, @Nullable NettyConfig nettyConfig,
      @Nullable TlsConfig tlsConfig) {
    boolean enableNativeTransports = nettyConfig != null && nettyConfig.isNativeTransportsEnabled();
    OsCheck.OSType operatingSystemType = OsCheck.getOperatingSystemType();
    if (enableNativeTransports
        && operatingSystemType == OsCheck.OSType.Linux
        && Epoll.isAvailable()) {
      _eventLoopGroup = new EpollEventLoopGroup();
      _channelClass = EpollSocketChannel.class;
      LOGGER.info("Using Epoll event loop");
    } else if (enableNativeTransports
        && operatingSystemType == OsCheck.OSType.MacOS
        && KQueue.isAvailable()) {
      _eventLoopGroup = new KQueueEventLoopGroup();
      _channelClass = KQueueSocketChannel.class;
      LOGGER.info("Using KQueue event loop");
    } else {
      _eventLoopGroup = new NioEventLoopGroup();
      _channelClass = NioSocketChannel.class;
      StringBuilder log = new StringBuilder("Using NIO event loop");
      if (operatingSystemType == OsCheck.OSType.Linux
          && enableNativeTransports) {
        log.append(", as Epoll is not available: ").append(Epoll.unavailabilityCause());
      } else if (operatingSystemType == OsCheck.OSType.MacOS
          && enableNativeTransports) {
        log.append(", as KQueue is not available: ").append(KQueue.unavailabilityCause());
      }
      LOGGER.info(log.toString());
    }

    _queryRouter = queryRouter;
    _brokerMetrics = brokerMetrics;
    _tlsConfig = tlsConfig;
    _threadLocalTSerializer = ThreadLocal.withInitial(() -> {
      try {
        return new TSerializer(new TCompactProtocol.Factory());
      } catch (TTransportException e) {
        throw new RuntimeException("Failed to initialize Thrift Serializer", e);
      }
    });
  }

  public void sendRequest(String rawTableName, AsyncQueryResponse asyncQueryResponse,
      ServerRoutingInstance serverRoutingInstance, InstanceRequest instanceRequest, long timeoutMs)
      throws Exception {
    byte[] requestBytes = _threadLocalTSerializer.get().serialize(instanceRequest);
    _serverToChannelMap.computeIfAbsent(serverRoutingInstance, ServerChannel::new)
        .sendRequest(rawTableName, asyncQueryResponse, serverRoutingInstance, requestBytes, timeoutMs);
  }

  public void connect(ServerRoutingInstance serverRoutingInstance)
      throws InterruptedException, TimeoutException {
    _serverToChannelMap.computeIfAbsent(serverRoutingInstance, ServerChannel::new).connect();
  }

  public void shutDown() {
    // Shut down immediately
    _eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
  }

  @ThreadSafe
  private class ServerChannel {
    final ServerRoutingInstance _serverRoutingInstance;
    final Bootstrap _bootstrap;
    // lock to protect channel as requests must be written into channel sequentially
    final ReentrantLock _channelLock = new ReentrantLock();
    Channel _channel;

    ServerChannel(ServerRoutingInstance serverRoutingInstance) {
      _serverRoutingInstance = serverRoutingInstance;
      PooledByteBufAllocator bufAllocator = PooledByteBufAllocator.DEFAULT;
      PooledByteBufAllocatorMetric metric = bufAllocator.metric();
      _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_USED_DIRECT_MEMORY, metric::usedDirectMemory);
      _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_USED_HEAP_MEMORY, metric::usedHeapMemory);
      _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_ARENAS_DIRECT, metric::numDirectArenas);
      _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_ARENAS_HEAP, metric::numHeapArenas);
      _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_CACHE_SIZE_SMALL, metric::smallCacheSize);
      _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_CACHE_SIZE_NORMAL, metric::normalCacheSize);
      _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_THREADLOCALCACHE, metric::numThreadLocalCaches);
      _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_CHUNK_SIZE, metric::chunkSize);

      _bootstrap = new Bootstrap().remoteAddress(serverRoutingInstance.getHostname(), serverRoutingInstance.getPort())
          .option(ChannelOption.ALLOCATOR, bufAllocator)
          .group(_eventLoopGroup).channel(_channelClass).option(ChannelOption.SO_KEEPALIVE, true)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              if (_tlsConfig != null) {
                // Add SSL handler first to encrypt and decrypt everything.
                ch.pipeline().addLast(
                    ChannelHandlerFactory.SSL, ChannelHandlerFactory.getClientTlsHandler(_tlsConfig, ch));
              }

              ch.pipeline().addLast(ChannelHandlerFactory.getLengthFieldBasedFrameDecoder());
              ch.pipeline().addLast(ChannelHandlerFactory.getLengthFieldPrepender());
              // NOTE: data table de-serialization happens inside this handler
              // Revisit if this becomes a bottleneck
              ch.pipeline().addLast(
                  ChannelHandlerFactory.getDataTableHandler(_queryRouter, _serverRoutingInstance, _brokerMetrics));
            }
          });
    }

    void sendRequest(String rawTableName, AsyncQueryResponse asyncQueryResponse,
        ServerRoutingInstance serverRoutingInstance, byte[] requestBytes, long timeoutMs)
        throws InterruptedException, TimeoutException {
      if (_channelLock.tryLock(timeoutMs, TimeUnit.MILLISECONDS)) {
        try {
          connectWithoutLocking();
          sendRequestWithoutLocking(rawTableName, asyncQueryResponse, serverRoutingInstance, requestBytes);
        } finally {
          _channelLock.unlock();
        }
      } else {
        throw new TimeoutException(CHANNEL_LOCK_TIMEOUT_MSG);
      }
    }

    void connectWithoutLocking()
        throws InterruptedException {
      if (_channel == null || !_channel.isActive()) {
        long startTime = System.currentTimeMillis();
        _channel = _bootstrap.connect().sync().channel();
        _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.NETTY_CONNECTION_CONNECT_TIME_MS,
            System.currentTimeMillis() - startTime);
      }
    }

    void sendRequestWithoutLocking(String rawTableName, AsyncQueryResponse asyncQueryResponse,
        ServerRoutingInstance serverRoutingInstance, byte[] requestBytes) {
      long startTimeMs = System.currentTimeMillis();
      _channel.writeAndFlush(Unpooled.wrappedBuffer(requestBytes)).addListener(f -> {
        int requestSentLatencyMs = (int) (System.currentTimeMillis() - startTimeMs);
        _brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.NETTY_CONNECTION_SEND_REQUEST_LATENCY,
            requestSentLatencyMs, TimeUnit.MILLISECONDS);
        asyncQueryResponse.markRequestSent(serverRoutingInstance, requestSentLatencyMs);
      });
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_REQUESTS_SENT, 1);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_BYTES_SENT, requestBytes.length);
    }

    void connect()
        throws InterruptedException, TimeoutException {
      if (_channelLock.tryLock(TRY_CONNECT_CHANNEL_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        try {
          connectWithoutLocking();
        } finally {
          _channelLock.unlock();
        }
      } else {
        throw new TimeoutException(CHANNEL_LOCK_TIMEOUT_MSG);
      }
    }
  }
}

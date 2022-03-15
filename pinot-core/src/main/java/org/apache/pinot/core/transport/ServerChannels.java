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
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
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
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.core.util.OsCheck;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransportException;


/**
 * The {@code ServerChannels} class manages the channels between broker to all the connected servers.
 * <p>There is only one channel between the broker and each connected server (we count OFFLINE and REALTIME as different
 * servers)
 */
@ThreadSafe
public class ServerChannels {
  public static final String CHANNEL_LOCK_TIMEOUT_MSG = "Timeout while acquiring channel lock";
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
    if (nettyConfig != null && nettyConfig.isNativeTransportsEnabled()
        && OsCheck.getOperatingSystemType() == OsCheck.OSType.Linux) {
      _eventLoopGroup = new EpollEventLoopGroup();
      _channelClass = EpollSocketChannel.class;
    } else if (nettyConfig != null && nettyConfig.isNativeTransportsEnabled()
        && OsCheck.getOperatingSystemType() == OsCheck.OSType.MacOS) {
      _eventLoopGroup = new KQueueEventLoopGroup();
      _channelClass = KQueueSocketChannel.class;
    } else {
      _eventLoopGroup = new NioEventLoopGroup();
      _channelClass = NioSocketChannel.class;
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
      _bootstrap = new Bootstrap().remoteAddress(serverRoutingInstance.getHostname(), serverRoutingInstance.getPort())
          .group(_eventLoopGroup).channel(_channelClass).option(ChannelOption.SO_KEEPALIVE, true)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              if (_tlsConfig != null) {
                attachSSLHandler(ch);
              }

              ch.pipeline()
                  .addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Integer.BYTES, 0, Integer.BYTES),
                      new LengthFieldPrepender(Integer.BYTES),
                      // NOTE: data table de-serialization happens inside this handler
                      // Revisit if this becomes a bottleneck
                      new DataTableHandler(_queryRouter, _serverRoutingInstance, _brokerMetrics));
            }
          });
    }

    private void attachSSLHandler(SocketChannel ch) {
      try {
        SslContextBuilder sslContextBuilder =
            SslContextBuilder.forClient().sslProvider(SslProvider.valueOf(_tlsConfig.getSslProvider()));

        if (_tlsConfig.getKeyStorePath() != null) {
          sslContextBuilder.keyManager(TlsUtils.createKeyManagerFactory(_tlsConfig));
        }

        if (_tlsConfig.getTrustStorePath() != null) {
          sslContextBuilder.trustManager(TlsUtils.createTrustManagerFactory(_tlsConfig));
        }

        ch.pipeline().addLast("ssl", sslContextBuilder.build().newHandler(ch.alloc()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void sendRequest(String rawTableName, AsyncQueryResponse asyncQueryResponse,
        ServerRoutingInstance serverRoutingInstance, byte[] requestBytes, long timeoutMs)
        throws Exception {
      if (_channelLock.tryLock(timeoutMs, TimeUnit.MILLISECONDS)) {
        try {
          sendRequest(rawTableName, asyncQueryResponse, serverRoutingInstance, requestBytes);
        } finally {
          _channelLock.unlock();
        }
      } else {
        throw new TimeoutException(CHANNEL_LOCK_TIMEOUT_MSG);
      }
    }

    private void sendRequest(String rawTableName, AsyncQueryResponse asyncQueryResponse,
        ServerRoutingInstance serverRoutingInstance, byte[] requestBytes)
        throws Exception {
      if (_channel == null || !_channel.isActive()) {
        long startTime = System.currentTimeMillis();
        _channel = _bootstrap.connect().sync().channel();
        _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.NETTY_CONNECTION_CONNECT_TIME_MS,
            System.currentTimeMillis() - startTime);
      }
      long sendRequestStartTimeMs = System.currentTimeMillis();
      _channel.writeAndFlush(Unpooled.wrappedBuffer(requestBytes)).addListener(f -> {
        long requestSentLatencyMs = System.currentTimeMillis() - sendRequestStartTimeMs;
        _brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.NETTY_CONNECTION_SEND_REQUEST_LATENCY,
            requestSentLatencyMs, TimeUnit.MILLISECONDS);
        asyncQueryResponse.markRequestSent(serverRoutingInstance, requestSentLatencyMs);
      });
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_REQUESTS_SENT, 1);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_BYTES_SENT, requestBytes.length);
    }
  }
}

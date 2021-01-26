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
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContextBuilder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.util.TlsUtils;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;


/**
 * The {@code ServerChannels} class manages the channels between broker to all the connected servers.
 * <p>There is only one channel between the broker and each connected server (we count OFFLINE and REALTIME as different
 * servers)
 */
@ThreadSafe
public class ServerChannels {
  private final QueryRouter _queryRouter;
  private final BrokerMetrics _brokerMetrics;
  private final ConcurrentHashMap<ServerRoutingInstance, ServerChannel> _serverToChannelMap = new ConcurrentHashMap<>();
  private final EventLoopGroup _eventLoopGroup = new NioEventLoopGroup();
  private final TlsConfig _tlsConfig;

  /**
   * Create an unsecured server channel
   *
   * @param queryRouter query router
   * @param brokerMetrics broker metrics
   */
  public ServerChannels(QueryRouter queryRouter, BrokerMetrics brokerMetrics) {
    this(queryRouter, brokerMetrics, null);
  }

  /**
   * Create a server channel with TLS config
   *
   * @param queryRouter query router
   * @param brokerMetrics broker metrics
   * @param tlsConfig TLS/SSL config
   */
  public ServerChannels(QueryRouter queryRouter, BrokerMetrics brokerMetrics, TlsConfig tlsConfig) {
    _queryRouter = queryRouter;
    _brokerMetrics = brokerMetrics;
    _tlsConfig = tlsConfig;
  }

  public void sendRequest(ServerRoutingInstance serverRoutingInstance, InstanceRequest instanceRequest)
      throws Exception {
    _serverToChannelMap.computeIfAbsent(serverRoutingInstance, ServerChannel::new).sendRequest(instanceRequest);
  }

  public void shutDown() {
    // Shut down immediately
    _eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
  }

  @ThreadSafe
  private class ServerChannel {
    final TSerializer _serializer = new TSerializer(new TCompactProtocol.Factory());
    final ServerRoutingInstance _serverRoutingInstance;
    final Bootstrap _bootstrap;
    Channel _channel;

    ServerChannel(ServerRoutingInstance serverRoutingInstance) {
      _serverRoutingInstance = serverRoutingInstance;
      _bootstrap = new Bootstrap().remoteAddress(serverRoutingInstance.getHostname(), serverRoutingInstance.getPort())
          .group(_eventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.SO_KEEPALIVE, true)
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
        SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();

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

    synchronized void sendRequest(InstanceRequest instanceRequest)
        throws Exception {
      if (_channel == null || !_channel.isActive()) {
        long startTime = System.currentTimeMillis();
        _channel = _bootstrap.connect().sync().channel();
        _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.NETTY_CONNECTION_CONNECT_TIME_MS,
            System.currentTimeMillis() - startTime);
      }
      byte[] requestBytes = _serializer.serialize(instanceRequest);
      _channel.writeAndFlush(Unpooled.wrappedBuffer(requestBytes), _channel.voidPromise());
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_REQUESTS_SENT, 1);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_BYTES_SENT, requestBytes.length);
    }
  }
}

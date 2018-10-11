/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.transport;

import com.linkedin.pinot.common.metrics.BrokerGauge;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.request.InstanceRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
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
  private final ConcurrentHashMap<Server, ServerChannel> _serverToChannelMap = new ConcurrentHashMap<>();
  private final EventLoopGroup _eventLoopGroup = new NioEventLoopGroup();

  public ServerChannels(QueryRouter queryRouter, BrokerMetrics brokerMetrics) {
    _queryRouter = queryRouter;
    _brokerMetrics = brokerMetrics;
  }

  public void sendRequest(Server server, InstanceRequest instanceRequest) throws Exception {
    _serverToChannelMap.computeIfAbsent(server, ServerChannel::new).sendRequest(instanceRequest);
  }

  public void shutDown() {
    // Shut down immediately
    _eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
  }

  @ThreadSafe
  private class ServerChannel {
    final TSerializer _serializer = new TSerializer(new TCompactProtocol.Factory());
    final Server _server;
    final Bootstrap _bootstrap;
    Channel _channel;

    ServerChannel(Server server) {
      _server = server;
      _bootstrap = new Bootstrap().remoteAddress(server.getHostName(), server.getPort())
          .group(_eventLoopGroup)
          .channel(NioSocketChannel.class)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              ch.pipeline()
                  .addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Integer.BYTES, 0, Integer.BYTES),
                      new LengthFieldPrepender(Integer.BYTES),
                      // NOTE: data table de-serialization happens inside this handler
                      // Revisit if this becomes a bottleneck
                      new DataTableHandler(_queryRouter, _server, _brokerMetrics));
            }
          });
    }

    synchronized void sendRequest(InstanceRequest instanceRequest) throws Exception {
      if (_channel == null || !_channel.isActive()) {
        long startTime = System.currentTimeMillis();
        _channel = _bootstrap.connect().sync().channel();
        _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.NETTY_CONNECTION_CONNECT_TIME_MS,
            System.currentTimeMillis() - startTime);
      }
      byte[] requestBytes = _serializer.serialize(instanceRequest);
      _channel.writeAndFlush(_channel.alloc().buffer(requestBytes.length).writeBytes(requestBytes),
          _channel.voidPromise());
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_REQUESTS_SENT, 1L);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_BYTES_SENT, requestBytes.length);
    }
  }
}

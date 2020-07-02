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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.query.scheduler.QueryScheduler;


/**
 * The {@code QueryServer} is the Netty server that runs on Pinot Server to handle the instance requests sent from Pinot
 * Brokers.
 */
public class QueryServer {
  private final int _port;
  private final QueryScheduler _queryScheduler;
  private final ServerMetrics _serverMetrics;

  private EventLoopGroup _bossGroup;
  private EventLoopGroup _workerGroup;
  private Channel _channel;

  public QueryServer(int port, QueryScheduler queryScheduler, ServerMetrics serverMetrics) {
    _port = port;
    _queryScheduler = queryScheduler;
    _serverMetrics = serverMetrics;
  }

  public void start() {
    _bossGroup = new NioEventLoopGroup();
    _workerGroup = new NioEventLoopGroup();
    try {
      ServerBootstrap serverBootstrap = new ServerBootstrap();
      _channel = serverBootstrap.group(_bossGroup, _workerGroup).channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              ch.pipeline()
                  .addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Integer.BYTES, 0, Integer.BYTES),
                      new LengthFieldPrepender(Integer.BYTES),
                      new InstanceRequestHandler(_queryScheduler, _serverMetrics));
            }
          }).bind(_port).sync().channel();
    } catch (Exception e) {
      // Shut down immediately
      _workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
      _bossGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
      throw new RuntimeException(e);
    }
  }

  public void shutDown() {
    try {
      _channel.close().sync();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      _workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
      _bossGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
    }
  }
}

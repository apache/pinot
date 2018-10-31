/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.transport.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.metrics.AggregatedMetricsRegistry;
import com.linkedin.pinot.transport.metrics.AggregatedTransportServerMetrics;
import com.linkedin.pinot.transport.metrics.NettyServerMetrics;
import com.yammer.metrics.core.MetricsRegistry;


/**
 *
 * TCP Server
 *
 *
 */
public class NettyTCPServer extends NettyServer {

  public NettyTCPServer(int port, RequestHandlerFactory handlerFactory, AggregatedMetricsRegistry registry, long defaultLargeQueryLatencyMs) {
    super(port, handlerFactory, registry, defaultLargeQueryLatencyMs);
  }

  public NettyTCPServer(int port, RequestHandlerFactory handlerFactory, AggregatedMetricsRegistry registry) {
    this(port, handlerFactory, registry, 100);
  }

  @Override
  protected ServerBootstrap getServerBootstrap() {
    ServerBootstrap b = new ServerBootstrap();
    b.group(_bossGroup, _workerGroup).channel(NioServerSocketChannel.class).childHandler(createChannelInitializer())
        .option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true);
    return b;
  }

  protected ChannelInitializer<SocketChannel> createChannelInitializer() {
    return new ServerChannelInitializer(_handlerFactory, _metricsRegistry, _metrics, _defaultLargeQueryLatencyMs);
  }

  /**
   * Server Channel Initializer
   */
  protected static class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final RequestHandlerFactory _handlerFactory;
    /**
     * If we note that we have higher overhead because of aggregation, we can use a single
     * metric for tracking server metrics. This can be done by doing
     * (a) Not using _globalMetrics ( = null); and
     * (b) All new instantiation of NettyServerMetrics will use the same name (or)
     * (c) Better Solution : Use one instance of NettyServerMetrics for all instantiations.
     */
    private final MetricsRegistry _registry;
    private final AggregatedTransportServerMetrics _globalMetrics;
    private final long _defaultLargeQueryLatencyMs;

    public ServerChannelInitializer(RequestHandlerFactory handlerFactory, MetricsRegistry registry,
        AggregatedTransportServerMetrics globalMetrics, long defaultLargeQueryLatencyMs) {
      _handlerFactory = handlerFactory;
      _registry = registry;
      _globalMetrics = globalMetrics;
      _defaultLargeQueryLatencyMs = defaultLargeQueryLatencyMs;
    }

    public ServerChannelInitializer(RequestHandlerFactory handlerFactory, MetricsRegistry registry,
        AggregatedTransportServerMetrics globalMetrics) {
      this(handlerFactory, registry, globalMetrics, 100);
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      LOGGER.info("Setting up Server channel, scheduler");
      ch.pipeline().addLast("decoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
      ch.pipeline().addLast("encoder", new LengthFieldPrepender(4));
      //ch.pipeline().addLast("logger", new LoggingHandler());
      // Create server metric for this handler and add to aggregate if present
      NettyServerMetrics serverMetric =
          new NettyServerMetrics(_registry, NettyTCPServer.class.getName() + "_" + Utils.getUniqueId() + "_");

      if (null != _globalMetrics) {
        _globalMetrics.addTransportClientMetrics(serverMetric);
      }

      ch.pipeline().addLast("request_handler",
          new NettyChannelInboundHandler(_handlerFactory.createNewRequestHandler(), serverMetric, _defaultLargeQueryLatencyMs));
    }
  }
}

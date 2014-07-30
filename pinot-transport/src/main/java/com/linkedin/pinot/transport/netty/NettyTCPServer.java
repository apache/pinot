package com.linkedin.pinot.transport.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.metrics.common.AggregatedMetricsRegistry;
import com.linkedin.pinot.transport.metrics.AggregatedTransportServerMetrics;
import com.linkedin.pinot.transport.metrics.NettyServerMetrics;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * 
 * TCP Server
 * 
 * @author Balaji Varadarajan
 *
 */
public class NettyTCPServer extends NettyServer {


  public NettyTCPServer(int port, RequestHandlerFactory handlerFactory, AggregatedMetricsRegistry registry) {
    super(port, handlerFactory, registry);
  }

  @Override
  protected ServerBootstrap getServerBootstrap() {
    ServerBootstrap b = new ServerBootstrap();
    b.group(_bossGroup, _workerGroup).channel(NioServerSocketChannel.class)
    .childHandler(createChannelInitializer())
    .option(ChannelOption.SO_BACKLOG, 128)
    .childOption(ChannelOption.SO_KEEPALIVE, true);
    return b;
  }

  protected ChannelInitializer<SocketChannel> createChannelInitializer()
  {
    return new ServerChannelInitializer(_handlerFactory, _metricsRegistry, _metrics);
  }

  /**
   * Server Channel Initializer
   */
  protected static class ServerChannelInitializer
  extends ChannelInitializer<SocketChannel>
  {
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

    public ServerChannelInitializer(RequestHandlerFactory handlerFactory, MetricsRegistry registry, AggregatedTransportServerMetrics globalMetrics)
    {
      _handlerFactory = handlerFactory;
      _registry = registry;
      _globalMetrics = globalMetrics;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      LOG.info("Setting up Server channel !!");
      ch.pipeline().addLast("decoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
      ch.pipeline().addLast("encoder", new LengthFieldPrepender(4));
      //ch.pipeline().addLast("logger", new LoggingHandler());
      // Create server metric for this handler and add to aggregate if present
      NettyServerMetrics serverMetric = new NettyServerMetrics(_registry, NettyTCPServer.class.getName() + "_" + Utils.getUniqueId() + "_");

      if ( null != _globalMetrics) {
        _globalMetrics.addTransportClientMetrics(serverMetric);
      }

      ch.pipeline().addLast("request_handler", new NettyChannelInboundHandler(_handlerFactory.createNewRequestHandler(), serverMetric));
    }
  }
}

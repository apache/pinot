package com.linkedin.pinot.transport.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public class NettyTCPServer extends NettyServer {

  public NettyTCPServer(int port, RequestHandlerFactory handlerFactory) {
    super(port, handlerFactory);
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
    return new ServerChannelInitializer(_handlerFactory);
  }

  /**
   * Server Channel Initializer
   */
  protected static class ServerChannelInitializer
  extends ChannelInitializer<SocketChannel>
  {
    private final RequestHandlerFactory _handlerFactory;

    public ServerChannelInitializer(RequestHandlerFactory handlerFactory)
    {
      _handlerFactory = handlerFactory;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      LOG.info("Setting up Server channel !!");
      ch.pipeline().addLast("decoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
      ch.pipeline().addLast("encoder", new LengthFieldPrepender(4));
      //ch.pipeline().addLast("logger", new LoggingHandler());
      ch.pipeline().addLast("request_handler", new NettyChannelInboundHandler(_handlerFactory.createNewRequestHandler()));
    }
  }
}

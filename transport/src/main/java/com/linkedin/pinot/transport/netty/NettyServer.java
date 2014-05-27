package com.linkedin.pinot.transport.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Netty Server abstraction. Server implementations are expected to implement the getServerBootstrap() abstract
 * method to configure the server protocol and setup handlers. The Netty server will then bind to the port and
 * listens to incoming connections on the port.
 */
public abstract class NettyServer implements Runnable {

  protected static Logger LOG = LoggerFactory.getLogger(NettyTCPClientConnection.class);

  /**
   * The request handler callback which processes the incoming request.
   * This method is executed by the Netty worker thread.
   */
  public static interface RequestHandler {
    /**
     * Callback for Servers to process the request and return the response.
     * The ownership of the request bytebuf resides with this class which is
     * responsible for releasing it.
     *
     * @param request Serialized request
     * @return Serialized response
     */
    public byte[] processRequest(ByteBuf request);
  }

  public static interface RequestHandlerFactory {

    /**
     * Request Handler Factory. The RequestHandler objects are not expected to be
     * thread-safe. Hence, we need a factory for the Channel Initializer to use for each incoming channel.
     * @return
     */
    public RequestHandler createNewRequestHandler();
  }

  /**
   * Server port
   */
  protected int _port;

  // Flag to indicate if shutdown has been requested
  protected AtomicBoolean _shutdownRequested = new AtomicBoolean(false);

  //TODO: Need configs to control number of threads
  protected final EventLoopGroup _bossGroup = new NioEventLoopGroup(5);
  protected final EventLoopGroup _workerGroup = new NioEventLoopGroup(20);

  protected Channel _channel = null;

  protected RequestHandlerFactory _handlerFactory;

  public NettyServer(int port, RequestHandlerFactory handlerFactory)
  {
    _port = port;
    _handlerFactory = handlerFactory;
  }

  @Override
  public void run()
  {
    try
    {
      ServerBootstrap bootstrap = getServerBootstrap();

      LOG.info("Binding to the server port !!");

      // Bind and start to accept incoming connections.
      ChannelFuture f = bootstrap.bind(_port).sync();
      _channel = f.channel();
      LOG.info("Server bounded to port :" + _port + ", Waiting for closing");
      f.channel().closeFuture().sync();
      LOG.info("Server boss channel is closed. Gracefully shutting down the server netty threads and pipelines");
    } catch (Exception e) {
      LOG.error("Got exception in the main server thread. Stopping !!", e);
    } finally {
      _bossGroup.shutdownGracefully();
      _workerGroup.shutdownGracefully();
    }
  }

  /**
   * Generate Protocol specific server bootstrap and return
   *
   */
  protected abstract ServerBootstrap getServerBootstrap();

  /**
   *  Shutdown gracefully
   */
  public void shutdownGracefully()
  {
    LOG.info("Shutdown requested in the server !!");
    _shutdownRequested.set(true);
    if ( null != _channel)
    {
      LOG.info("Closing the server boss channel");
      _channel.close();
    }
  }

  public static class NettyChannelInboundHandler
  extends ChannelInboundHandlerAdapter
  implements ChannelFutureListener
  {
    private final RequestHandler _handler;
    public NettyChannelInboundHandler(RequestHandler handler)
    {
      _handler = handler;
    }

    public enum State
    {
      INIT,
      REQUEST_RECEIVED,
      RESPONSE_WRITTEN,
      RESPONSE_SENT,
      EXCEPTION
    }

    /**
     * Server Channel Handler State
     */
    private State _state = State.INIT;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
      LOG.info("Request received by server !!");
      _state = State.REQUEST_RECEIVED;
      ByteBuf request = (ByteBuf)msg;
      byte[] response = _handler.processRequest(request);
      ByteBuf responseBuf = Unpooled.wrappedBuffer(response);
      ChannelFuture f = ctx.writeAndFlush(responseBuf);
      _state = State.RESPONSE_WRITTEN;
      f.addListener(this);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
      _state = State.EXCEPTION;
      LOG.error("Got exception in the channel handler", cause);
      ctx.close();
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      LOG.info("Response has been sent !!");
      _state = State.RESPONSE_SENT;
    }
  }
}

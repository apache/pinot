package com.linkedin.pinot.transport.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.concurrent.atomic.AtomicReference;

import com.linkedin.pinot.transport.common.ServerInstance;

/**
 * TCP based Netty Client Connection
 */
public class NettyTCPClientConnection extends NettyClientConnection
implements ChannelFutureListener
{
  /**
   * Channel Inbound Handler for receiving response asynchronously
   */
  private NettyClientConnectionHandler _handler = null;

  /**
   * Handle to the future corresponding to the pending request
   */
  private final AtomicReference<ResponseFuture> _outstandingFuture;

  public NettyTCPClientConnection(ServerInstance server, EventLoopGroup eventGroup)
  {
    super(server,eventGroup);
    _handler = new NettyClientConnectionHandler();
    _outstandingFuture = new AtomicReference<ResponseFuture>();
    init();
  }

  private void init()
  {
    _bootstrap = new Bootstrap();
    _bootstrap.group(_eventGroup).channel(NioSocketChannel.class).handler(new ChannelHandlerInitializer(_handler));
  }

  /**
   * Used to validate if the connection state transition is valid.
   * @param nextState
   */
  private void checkTransition(State nextState)
  {
    if (! _connState.isValidTransition(nextState))
    {
      throw new IllegalStateException("Wrong transition :" + _connState + " -> " + nextState);
    }
  }

  /**
   * Open a connection
   */
  @Override
  public boolean connect()
  {
    try
    {
      checkTransition(State.CONNECTED);
      //Connect synchronously. At the end of this line, _channel should have been set
      ChannelFuture f = _bootstrap.connect(_server.getHostname(), _server.getPort()).sync();
      f.await();
      _connState = State.CONNECTED;
      return true;
    } catch (InterruptedException ie) {
      LOG.info("Got interrupted exception when connecting to server :" + _server,ie);
    }
    return false;
  }

  /**
   * Called by the channel initializer to set the underlying channel reference.
   * @param channel
   */
  private void setChannel(Channel channel)
  {
    _channel = channel;
  }

  @Override
  public ResponseFuture sendRequest(ByteBuf serializedRequest)
  {
    checkTransition(State.REQUEST_WRITTEN);
    //byte[] b2 = new byte[serializedRequest.readableBytes()];
    //serializedRequest.readBytes(b2);
    //String req = new String(b2);
    _outstandingFuture.set(new ResponseFuture());
    ChannelFuture f = _channel.writeAndFlush(serializedRequest);
    _connState = State.REQUEST_WRITTEN;
    f.addListener(this);
    return _outstandingFuture.get();
  }

  @Override
  public void operationComplete(ChannelFuture future) throws Exception
  {
    checkTransition(State.REQUEST_SENT);
    LOG.info("Request has been sent !!");
    _connState = State.REQUEST_SENT;
  }

  /**
   * Channel Handler for incoming response.
   * 
   */
  public class NettyClientConnectionHandler
  extends ChannelInboundHandlerAdapter
  {
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
      LOG.info("Client Channel in inactive state (closed).  !!");
      if (null != _outstandingFuture.get())
      {
        _outstandingFuture.get().processError(new Exception("Client Channel is closed !!"));
      }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
      setChannel(ctx.channel());
      super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      ByteBuf result = (ByteBuf)msg;
      checkTransition(State.GOT_RESPONSE);
      _connState = State.GOT_RESPONSE;
      _outstandingFuture.get().processResponse(result);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      //LOG.error("Got exception when processing the channel. Closing the channel", cause);
      checkTransition(State.ERROR);
      _connState = State.ERROR;
      _outstandingFuture.get().processError(cause);
      ctx.close();
    }
  }

  /**
   * Netty Client Channel Initializer responsible for setting the pipeline
   */
  public class ChannelHandlerInitializer extends ChannelInitializer<SocketChannel> {

    private final ChannelHandler _handler;

    public ChannelHandlerInitializer(ChannelHandler handler)
    {
      _handler = handler;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception
    {
      ChannelPipeline pipeline = ch.pipeline();
      /**
       * We will use a length prepended payload to defragment TCP fragments.
       */
      pipeline.addLast("decoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
      pipeline.addLast("encoder", new LengthFieldPrepender(4));
      //pipeline.addLast("logger", new LoggingHandler());
      pipeline.addLast("handler", _handler);
      LOG.info("Server Channel pipeline setup. Pipeline:" + ch.pipeline().names());
    }
  }

  @Override
  public void close() throws InterruptedException {
    LOG.info("Client channel close() called. Closing client channel !!");
    if ( null != _channel)
    {
      _channel.close().sync();
    }
    if ( null != _eventGroup)
    {
      _eventGroup.shutdownGracefully();
    }
  }
}
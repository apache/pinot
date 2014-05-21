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
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ListenableFuture;
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
  }

  @Override
  public void init()
  {
    _bootstrap = new Bootstrap();
    _bootstrap.group(_eventGroup).channel(NioSocketChannel.class).handler(_handler);
    _connState = State.INIT;
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
      _bootstrap.connect(_server.getHostname(), _server.getPort()).sync();
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
  public ListenableFuture<ByteBuf> sendRequest(ByteBuf serializedRequest)
  {
    _outstandingFuture.set(new ResponseFuture());
    checkTransition(State.REQUEST_WRITTEN);
    ChannelFuture f = _channel.write(serializedRequest);
    _connState = State.REQUEST_WRITTEN;
    f.addListener(this);
    return _outstandingFuture.get();
  }

  @Override
  public void operationComplete(ChannelFuture future) throws Exception
  {
    checkTransition(State.REQUEST_SENT);
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
    private final ByteToMessageDecoder _decoder;
    private final LengthFieldPrepender _encoder;

    public ChannelHandlerInitializer(ChannelHandler handler)
    {
      _handler = handler;
      _decoder = new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 8);
      _encoder = new LengthFieldPrepender(8);
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception
    {
      ChannelPipeline pipeline = ch.pipeline();
      setChannel(ch);
      /**
       * We will use a length prepended payload to defragment TCP fragments.
       */
      pipeline.addLast("BytoToMessageDecoder", _decoder);
      pipeline.addLast("MessageToByteEncoder", _encoder);
      pipeline.addLast("handler", _handler);
    }
  }
}
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
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.linkedin.pinot.metrics.common.MetricsHelper;
import com.linkedin.pinot.metrics.common.MetricsHelper.TimerContext;
import com.linkedin.pinot.transport.common.ServerInstance;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;

/**
 * TCP based Netty Client Connection
 * @author Balaji Varadarajan
 */
public class NettyTCPClientConnection extends NettyClientConnection
implements ChannelFutureListener
{
  /**
   * Channel Inbound Handler for receiving response asynchronously
   */
  private NettyClientConnectionHandler _handler = null;

  private NettyClientMetrics _clientMetric = null;

  /**
   * Handle to the future corresponding to the pending request
   */
  private final AtomicReference<ResponseFuture> _outstandingFuture;

  private long _lastRequsetSizeInBytes;
  private long _lastResponseSizeInBytes;
  private TimerContext _lastSendRequestLatency;
  private TimerContext _lastResponseLatency;

  // Timeout object corresponding to the outstanding request
  private Timeout _lastRequestTimeout;
  private long _lastRequestTimeoutMS;
  private long _lastRequestId;

  public NettyTCPClientConnection(ServerInstance server, EventLoopGroup eventGroup, Timer timer, NettyClientMetrics metric)
  {
    super(server,eventGroup, timer);
    _handler = new NettyClientConnectionHandler();
    _outstandingFuture = new AtomicReference<ResponseFuture>();
    _clientMetric = metric;
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
      TimerContext t = MetricsHelper.startTimer();
      ChannelFuture f = _bootstrap.connect(_server.getHostname(), _server.getPort()).sync();
      f.await();
      t.stop();
      _connState = State.CONNECTED;
      _clientMetric.addConnectStats(t.getLatencyMs());
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
  public ResponseFuture sendRequest(ByteBuf serializedRequest, long requestId, long timeoutMS)
  {
    checkTransition(State.REQUEST_WRITTEN);

    //Metrics update
    _lastRequsetSizeInBytes = serializedRequest.readableBytes();
    _lastSendRequestLatency = MetricsHelper.startTimer();
    _lastResponseLatency = MetricsHelper.startTimer();

    _outstandingFuture.set(new ResponseFuture(_server));
    _lastRequestTimeoutMS = timeoutMS;
    _lastRequestId = requestId;

    /**
     * Start the timer before sending the request.
     * That way, both cases of timeout (request writing to send-buffer and response timeout)
     * can be treated as single timeout condition and handled in the same way
     */
    _lastRequestTimeout = _timer.newTimeout(new ReadTimeoutHandler(), _lastRequestTimeoutMS, TimeUnit.MILLISECONDS);

    ChannelFuture f = null;
    try
    {
      f = _channel.writeAndFlush(serializedRequest);
      _connState = State.REQUEST_WRITTEN;
      f.addListener(this);
    } catch (Exception e) {
      /**
       * This might not be needed as if we get an exception, channelException() or channelClosed() would
       * have been called which would have set error response but defensively setting. Need to check if
       * this is needed
       */
      _outstandingFuture.get().onError(e);
    }

    return _outstandingFuture.get();
  }

  @Override
  public void operationComplete(ChannelFuture future) throws Exception
  {
    checkTransition(State.REQUEST_SENT);
    LOG.info("Request {} has been sent to server {} !!", _lastRequestId, _server);
    _connState = State.REQUEST_SENT;
    _lastSendRequestLatency.stop();
  }

  protected void cancelLastRequestTimeout()
  {
    if (null != _lastRequestTimeout)
    {
      _lastRequestTimeout.cancel(); //If task is already executed, no side-effect
      _lastRequestTimeout = null;
    }
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
      cancelLastRequestTimeout();
      if (null != _outstandingFuture.get())
      {
        _outstandingFuture.get().onError(new Exception("Client Channel is closed !!"));
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

      //Cancel outstanding timer
      cancelLastRequestTimeout();

      ByteBuf result = (ByteBuf)msg;
      checkTransition(State.GOT_RESPONSE);
      _lastResponseSizeInBytes = result.readableBytes();
      _lastResponseLatency.stop();
      _connState = State.GOT_RESPONSE;
      _outstandingFuture.get().onSuccess(result);
      _clientMetric.addRequestResponseStats(_lastRequsetSizeInBytes, 1, _lastResponseSizeInBytes, false, _lastSendRequestLatency.getLatencyMs(), _lastResponseLatency.getLatencyMs());

      if ( null != _requestCallback) {
        _requestCallback.onSuccess(null);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

      //Cancel outstanding timer
      cancelLastRequestTimeout();

      _lastResponseLatency.stop();
      //LOG.error("Got exception when processing the channel. Closing the channel", cause);
      checkTransition(State.ERROR);
      _connState = State.ERROR;
      _outstandingFuture.get().onError(cause);
      _clientMetric.addRequestResponseStats(_lastRequsetSizeInBytes, 1, _lastResponseSizeInBytes, true, _lastSendRequestLatency.getLatencyMs(), _lastResponseLatency.getLatencyMs());
      ctx.close();

      if ( null != _requestCallback) {
        _requestCallback.onError(cause);
      }
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
  }

  /**
   * Timer task responsible for closing the connection on timeout
   * @author Balaji Varadarajan
   *
   */
  public class ReadTimeoutHandler implements TimerTask
  {

    @Override
    public void run(Timeout timeout) throws Exception {
      LOG.error("Request (" + _lastRequestId + ") to server " +  _server +  " timed-out waiting for response. Closing the channel !!");
      close();
    }
  }
}
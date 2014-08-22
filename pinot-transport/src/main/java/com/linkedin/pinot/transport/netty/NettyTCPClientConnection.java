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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.metrics.MetricsHelper.TimerContext;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;


/**
 * TCP based Netty Client Connection
 * @author Balaji Varadarajan
 */
public class NettyTCPClientConnection extends NettyClientConnection  {
  /**
   * Channel Inbound Handler for receiving response asynchronously
   */
  private NettyClientConnectionHandler _handler = null;

  private NettyClientMetrics _clientMetric = null;

  /**
   * Handle to the future corresponding to the pending request
   */
  private final AtomicReference<ResponseFuture> _outstandingFuture;

  // Connection Id
  private final long _connId;
  
  private long _lastRequsetSizeInBytes;
  private long _lastResponseSizeInBytes;
  private TimerContext _lastSendRequestLatency;
  private TimerContext _lastResponseLatency;

  // Timeout object corresponding to the outstanding request
  private volatile Timeout _lastRequestTimeout;
  private volatile long _lastRequestTimeoutMS;
  private volatile long _lastRequestId;
  private volatile Throwable _lastError;
  
  // COnnection Id generator
  private static final AtomicLong _connIdGen = new AtomicLong(0);
  
  // Channel Setting notification
  private final CountDownLatch _channelSet = new CountDownLatch(1);
  
  public NettyTCPClientConnection(ServerInstance server, EventLoopGroup eventGroup, Timer timer,
      NettyClientMetrics metric) {
    super(server, eventGroup, timer);
    _handler = new NettyClientConnectionHandler();
    _outstandingFuture = new AtomicReference<ResponseFuture>();
    _clientMetric = metric;
    _connId = _connIdGen.incrementAndGet();
    init();
  }

  private void init() {
    _bootstrap = new Bootstrap();
    _bootstrap.group(_eventGroup).channel(NioSocketChannel.class).handler(new ChannelHandlerInitializer(_handler));
  }

  /**
   * Used to validate if the connection state transition is valid.
   * @param nextState
   */
  private void checkTransition(State nextState) {
    if (!_connState.isValidTransition(nextState)) {
      throw new IllegalStateException("Wrong transition :" + _connState + " -> " + nextState);
    }
  }

  /**
   * Open a connection
   */
  @Override
  public boolean connect() {
    try {
      checkTransition(State.CONNECTED);
      //Connect synchronously. At the end of this line, _channel should have been set
      TimerContext t = MetricsHelper.startTimer();
      ChannelFuture f =  _bootstrap.connect(_server.getHostname(), _server.getPort()).sync();
      /**
       * Waiting for future alone does not guarantee that _channel is set. _channel is set
       * only when the channelActive() async callback runs. So we should also wait for it.
       */
      f.get();
      _channelSet.await();
      t.stop();
      
      _connState = State.CONNECTED;
      _clientMetric.addConnectStats(t.getLatencyMs());
      return true;
    } catch (Exception ie) {
      LOG.error("Got exception when connecting to server :" + _server, ie);
    } 
    return false;
  }

  /**
   * Called by the channel initializer to set the underlying channel reference.
   * @param channel
   */
  private void setChannel(Channel channel) {
    _channel = channel;
    _channelSet.countDown();
    LOG.info("Setting channel for connection id (" + _connId + "). Is channel null ? " + (null == _channel));
  }

  @Override
  public ResponseFuture sendRequest(ByteBuf serializedRequest, long requestId, long timeoutMS) {
    checkTransition(State.REQUEST_WRITTEN);

    //Metrics update
    _lastRequsetSizeInBytes = serializedRequest.readableBytes();
    _lastSendRequestLatency = MetricsHelper.startTimer();
    _lastResponseLatency = MetricsHelper.startTimer();

    _outstandingFuture.set(new ResponseFuture(_server, "Response Future for request " + requestId + " to server " + _server));
    _lastRequestTimeoutMS = timeoutMS;
    _lastRequestId = requestId;
    _lastError = null;

    /**
     * Start the timer before sending the request.
     * That way, both cases of timeout (request writing to send-buffer and response timeout)
     * can be treated as single timeout condition and handled in the same way
     */
    _lastRequestTimeout = _timer.newTimeout(new ReadTimeoutHandler(), _lastRequestTimeoutMS, TimeUnit.MILLISECONDS);
    ChannelFuture f = null;
    try {
      
      _connState = State.REQUEST_WRITTEN;
      f = _channel.writeAndFlush(serializedRequest);
      /**
       * IMPORTANT:
       * There could be 2 netty threads one running the below code and another for response/error handling.
       * simultaneously. Netty does not provide guarantees around this. So in worst case, the thread that
       * is flushing request could block for sometime and gets executed after the response is obtained.
       * We should checkin the connection to the pool only after all outstanding callbacks are complete.
       * We do this by trancking the connection state. If we detect that response/error is already obtained,
       * we then do the process of checking back the connection to the pool or destroying (if error)
       */
      synchronized(_handler)
      {
        _lastSendRequestLatency.stop();

        if ( _connState == State.REQUEST_WRITTEN)
        {
          _connState = State.REQUEST_SENT; 
        } else {
          LOG.info("Response/Error already arrived !! Checking-in/destroying the connection");
          if ( _connState == State.GOT_RESPONSE)
          {
            if (null != _requestCallback) {
              _requestCallback.onSuccess(null);
            }
          } else if ( _connState == State.ERROR){
            if (null != _requestCallback) {
              _requestCallback.onError(_lastError);
            }
          } else {
            throw new IllegalStateException("Invalid connection State (" + _connState 
                                             + ") when sending request to  server " + _server);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Got exception sending the request to server (" + _server + ") id :" + _connId, e);
      
      /**
       * This might not be needed as if we get an exception, channelException() or channelClosed() would
       * have been called which would have set error response but defensively setting. Need to check if
       * this is needed
       */
      _outstandingFuture.get().onError(e);
      
      if (null != _requestCallback) {
        _requestCallback.onError(e);
      }
      _lastSendRequestLatency.stop();
    } 
    return _outstandingFuture.get();
  }

  protected void cancelLastRequestTimeout() {
    if (null != _lastRequestTimeout) {
      _lastRequestTimeout.cancel(); //If task is already executed, no side-effect
      _lastRequestTimeout = null;
    }
  }

  /**
   * Channel Handler for incoming response.
   * 
   */
  public class NettyClientConnectionHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      LOG.info("Client Channel to server ({}) (id = {}) in inactive state (closed).  !!", _server, _connId);
      Exception ex = new Exception("Client Channel to server (" + _server + ") is in inactive state (closed) !!");
      closeOnError(ctx, ex);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      LOG.info("Client Channel to server ({}) (id = {}) is active.", _server, _connId);
      setChannel(ctx.channel());
      super.channelActive(ctx);
    }

    @Override
    public synchronized void channelRead(ChannelHandlerContext ctx, Object msg) {
      
      //Cancel outstanding timer
      cancelLastRequestTimeout();

      ByteBuf result = (ByteBuf) msg;
      //checkTransition(State.GOT_RESPONSE);
      _lastResponseSizeInBytes = result.readableBytes();
      _lastResponseLatency.stop();

      State prevState = _connState;
      _connState = State.GOT_RESPONSE;
      
      _outstandingFuture.get().onSuccess(result);
      _clientMetric.addRequestResponseStats(_lastRequsetSizeInBytes, 1, _lastResponseSizeInBytes, false,
          _lastSendRequestLatency.getLatencyMs(), _lastResponseLatency.getLatencyMs());

      /**
       * IMPORTANT:
       * There could be 2 netty threads one running the sendRequest() code (the ones after flush()
       * and another for response/error handling (this one)
       * simultaneously. Netty does not provide guarantees around this. So in worst case, the thread that
       * is flushing request could block for sometime and gets executed after the response is obtained.
       * We should checkin the connection to the pool only after all outstanding callbacks are complete.
       * We do this by trancking the connection state. If we detect that response/error arrives before sendRequest
       * completes, we will let the sendRequest to checkin/destroy the connection.
       */
      if ((null != _requestCallback) && (prevState == State.REQUEST_SENT)) {
        _requestCallback.onSuccess(null);
      }
      
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Got exception in the channel !", cause);
      closeOnError(ctx, cause);
    }
    
    private synchronized void closeOnError(ChannelHandlerContext ctx, Throwable cause)
    {
      if ( _connState != State.ERROR)
      {
        //Cancel outstanding timer
        cancelLastRequestTimeout();

        if ( null != _lastResponseLatency)
          _lastResponseLatency.stop();

        //LOG.error("Got exception when processing the channel. Closing the channel", cause);
        checkTransition(State.ERROR);
        
        if ( null != _outstandingFuture.get())
        {
          _outstandingFuture.get().onError(cause);
        }
        _clientMetric.addRequestResponseStats(_lastRequsetSizeInBytes, 1, _lastResponseSizeInBytes, true,
                (null == _lastSendRequestLatency) ? 0 : _lastSendRequestLatency.getLatencyMs(), 
                (null == _lastSendRequestLatency ) ? 0 :_lastResponseLatency.getLatencyMs());
        

        /**
         * IMPORTANT:
         * There could be 2 netty threads one running the sendRequest() code (the ones after flush()
         * and another for response/error handling (this one)
         * simultaneously. Netty does not provide guarantees around this. So in worst case, the thread that
         * is flushing request could block for sometime and gets executed after the response is obtained.
         * We should checkin the connection to the pool only after all outstanding callbacks are complete.
         * We do this by trancking the connection state. If we detect that response/error arrives before sendRequest
         * completes, we will let the sendRequest to checkin/destroy the connection.
         */
        if ((null != _requestCallback) && (_connState == State.REQUEST_SENT)) {
          LOG.info("Discarding the connection !!");
          _requestCallback.onError(cause);
        }

        _connState = State.ERROR;

        ctx.close();

      }
    }
  }

  /**
   * Netty Client Channel Initializer responsible for setting the pipeline
   */
  public class ChannelHandlerInitializer extends ChannelInitializer<SocketChannel> {

    private final ChannelHandler _handler;

    public ChannelHandlerInitializer(ChannelHandler handler) {
      _handler = handler;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
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
    if (null != _channel) {
      _channel.close().sync();
    }
  }

  /**
   * Timer task responsible for closing the connection on timeout
   * @author Balaji Varadarajan
   *
   */
  public class ReadTimeoutHandler implements TimerTask {

    @Override
    public void run(Timeout timeout) throws Exception {
      String message =
          "Request (" + _lastRequestId + ") to server " + _server
              + " timed-out waiting for response. Closing the channel !!";
      LOG.error(message);
      Exception e = new Exception(message);
      _outstandingFuture.get().onError(e);
      close();
    }
  }
}

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
package com.linkedin.pinot.transport.netty;

import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.metrics.MetricsHelper.TimerContext;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
import java.net.ConnectException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/**
 * TCP based Netty Client Connection.
 *
 * Request and Response have the following format
 *
 * 0                                                         31
 * ------------------------------------------------------------
 * |                  Length ( 32 bits)                       |
 * |                 Payload (Request/Response)               |
 * |                    ...............                       |
 * |                    ...............                       |
 * |                    ...............                       |
 * |                    ...............                       |
 * ------------------------------------------------------------
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

  private long _lastRequsetSizeInBytes;
  private long _lastResponseSizeInBytes;
  private TimerContext _lastSendRequestLatency;
  private TimerContext _lastResponseLatency;

  // Timeout object corresponding to the outstanding request
  private volatile Timeout _lastRequestTimeout;
  private volatile long _lastRequestTimeoutMS;
  private volatile long _lastRequestId;
  private volatile Throwable _lastError;
  private volatile boolean _selfClose = false;

  // COnnection Id generator
  private static final AtomicLong _connIdGen = new AtomicLong(0);

  // Channel Setting notification
  private final CountDownLatch _channelSet = new CountDownLatch(1);

  public NettyTCPClientConnection(ServerInstance server, EventLoopGroup eventGroup, Timer timer,
      NettyClientMetrics metric) {
    super(server, eventGroup, timer,_connIdGen.incrementAndGet() );
    _handler = new NettyClientConnectionHandler();
    _outstandingFuture = new AtomicReference<ResponseFuture>();
    _clientMetric = metric;
    init();
  }

  private void init() {
    _bootstrap = new Bootstrap();
    _bootstrap.group(_eventGroup).channel(NioSocketChannel.class).handler(new ChannelHandlerInitializer(_handler));
  }

  protected void setSelfClose(boolean selfClose) {
    _selfClose = selfClose;
  }

  protected boolean isSelfClose() {
    return _selfClose;
  }

  /**
   * Used to validate if the connection state transition is valid.
   * @param nextState
   */
  private void checkTransition(State nextState) {
    if (!_connState.isValidTransition(nextState)) {
      throw new IllegalStateException("Wrong transition :" + _connState + " -> " + nextState + ", connId:" + getConnId());
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
      if (ie instanceof ConnectException && ie.getMessage() != null && ie.getMessage().startsWith("Connection refused")) {
        // Most common case when a server is down. Don't print the entire stack and fill the logs.
        LOGGER.info("Could not connect to server {}:{} connId:{}", _server, ie.getMessage(), getConnId());
      } else {
        LOGGER.error("Got exception when connecting to server {} connId {}", _server, ie, getConnId());
      }
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
    LOGGER.info("Setting channel for connection id ({}) to server {}. Is channel null? {}",_connId, _server, (null == _channel));
  }

  @Override
  public ResponseFuture sendRequest(ByteBuf serializedRequest, long requestId, long timeoutMS) {
    checkTransition(State.REQUEST_WRITTEN);

    //Metrics update
    _lastRequsetSizeInBytes = serializedRequest.readableBytes();
    _lastSendRequestLatency = MetricsHelper.startTimer();
    _lastResponseLatency = MetricsHelper.startTimer();

    _outstandingFuture.set(new ResponseFuture(_server, "Server response future for reqId " + requestId + " to server " + _server + " connId " + getConnId()));
    _lastRequestTimeoutMS = timeoutMS;
    _lastRequestId = requestId;
    _lastError = null;

    /**
     * Start the timer before sending the request.
     * That way, both cases of timeout (request writing to send-buffer and response timeout)
     * can be treated as single timeout condition and handled in the same way
     */
    _lastRequestTimeout = _timer.newTimeout(new ReadTimeoutHandler(), _lastRequestTimeoutMS, TimeUnit.MILLISECONDS);
    try {

      _connState = State.REQUEST_WRITTEN;
      _channel.writeAndFlush(serializedRequest);
      /**
       * IMPORTANT:
       * There could be 2 netty threads one running the below code and another for response/error handling.
       * simultaneously. Netty does not provide guarantees around this. So in worst case, the thread that
       * is flushing request could block for sometime and gets executed after the response is obtained.
       * We should checkin the connection to the pool only after all outstanding callbacks are complete.
       * We do this by tracking the connection state. If we detect that response/error is already obtained,
       * we then do the process of checking back the connection to the pool or destroying (if error)
       */
      synchronized(_handler)
      {
        _lastSendRequestLatency.stop();

        if ( _connState == State.REQUEST_WRITTEN)
        {
          _connState = State.REQUEST_SENT;
        } else {
          LOGGER.info("Response/Error already arrived !! Checking-in/destroying the connection to server {}, connId {}", _server, getConnId());
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
                                             + ") when sending request to  server " + _server + ", connId " + getConnId());
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Got exception sending the request to server ({}) id {}", _server, getConnId(), e);

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

  // Having a toString method here is useful when logging stuff from AsyncPoolImpl
  @Override
  public String toString() {
    return "Server:" + _server + ",State:" + _connState + ",connId:" + getConnId();
  }

  /**
   * Channel Handler for incoming response.
   *
   */
  public class NettyClientConnectionHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      // called when:
      // 1. idle server restart
      // 2. Also when an idle connection is closed by broker. In that case, self-close is set to true.
      LOGGER.info("Client Channel to server ({}) (id = {}) in inactive state (closed).  !!", _server, _connId);
      Exception ex = new Exception("Client Channel to server (" + _server + ") is in inactive state (closed) !!");
      closeOnError(ctx, ex);
      releaseResources();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      LOGGER.info("Client Channel to server ({}) (id = {}) is active.", _server, _connId);
      setChannel(ctx.channel());
      super.channelActive(ctx);
    }

    @Override
    public synchronized void channelRead(ChannelHandlerContext ctx, Object msg) {
      ByteBuf responseByteBuf = (ByteBuf) msg;
      try {
        // Cancel outstanding timer
        cancelLastRequestTimeout();

        //checkTransition(State.GOT_RESPONSE);
        _lastResponseLatency.stop();

        int numReadableBytes = responseByteBuf.readableBytes();
        byte[] responseBytes = new byte[numReadableBytes];
        if (numReadableBytes > 0) {
          responseByteBuf.readBytes(responseBytes);
        }
        _lastResponseSizeInBytes = numReadableBytes;

        State prevState = _connState;
        _connState = State.GOT_RESPONSE;

        _outstandingFuture.get().onSuccess(responseBytes);
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
      } finally {
        responseByteBuf.release();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      // Called from netty when we get an exception in the channel. Typically this is when the server restarts
      // and we have connections that may or may not have outstanding requests. Connections could also be in the
      // process of being set up when we get this exception.
      LOGGER.info("Got exception in the channel to {}, connId {}, cause:{}", _server, getConnId(), cause.getMessage());
      closeOnError(ctx, cause);
      releaseResources();
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
          LOGGER.info("Discarding the connection to {} connId {}", _server, getConnId());
          _requestCallback.onError(cause);
        }

        _connState = State.ERROR;

        ctx.close();

      }
    }
  }

  protected void releaseResources() {

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
      LOGGER.info("Server Channel pipeline setup. Pipeline:" + ch.pipeline().names());
    }
  }

  @Override
  public void close() throws InterruptedException {
    LOGGER.info("Closing client channel to {} connId {}", _server, getConnId());
    if (null != _channel) {
      _channel.close().sync();
      setSelfClose(true);
    }
  }
  /**
   * Timer task responsible for closing the connection on timeout
   *
   */
  public class ReadTimeoutHandler implements TimerTask {

    @Override
    public void run(Timeout timeout) throws Exception {
      String message =
          "Request (" + _lastRequestId + ") to server " + _server + " connId " + getConnId()
              + " timed-out waiting for response. Closing the channel !!";
      LOGGER.warn(message);
      Exception e = new Exception(message);
      _outstandingFuture.get().onError(e);
      close();
    }
  }
}

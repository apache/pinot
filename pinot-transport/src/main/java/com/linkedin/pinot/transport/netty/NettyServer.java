/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.metrics.AggregatedMetricsRegistry;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.metrics.MetricsHelper.TimerContext;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.transport.metrics.AggregatedTransportServerMetrics;
import com.linkedin.pinot.transport.metrics.NettyServerMetrics;


/**
 * A Netty Server abstraction. Server implementations are expected to implement the getServerBootstrap() abstract
 * method to configure the server protocol and setup handlers. The Netty server will then bind to the port and
 * listens to incoming connections on the port.
 *
 */
public abstract class NettyServer implements Runnable {

  protected static Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);

  // Server Metrics Group Name Prefix in Metrics Registry
  public static final String AGGREGATED_SERVER_METRICS_NAME = "Server_Global_Metric_";

  /**
   * The request handler callback which processes the incoming request.
   * This method is executed by the Netty worker thread.
   */
  public static interface RequestHandler {
    /**
     * Callback for Servers to process the request and return the response.
     * The ownership of the request bytebuf resides with the caler (NettyServer).
     * This callback is not expected to call {@Bytebuf.release()} on request
     * The ownership of the request byteBuf lies with the caller.
     *
     * The implementation MUST not throw any runtime exceptions. In case of errors,
     * the implementation is expected to construct and return an error response.
     * If the implementation throws runtime exceptions, then the underlying connection
     * will be terminated.
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

  // Flag to indicate if shutdown has been completed
  protected AtomicBoolean _shutdownComplete = new AtomicBoolean(false);

  //TODO: Need configs to control number of threads
  protected final EventLoopGroup _bossGroup = new NioEventLoopGroup(1);
  protected final EventLoopGroup _workerGroup = new NioEventLoopGroup(20);

  // Netty Channel
  protected Channel _channel = null;

  // Factory for generating request Handlers
  protected RequestHandlerFactory _handlerFactory;

  // Aggregated Metrics Registry
  protected final AggregatedMetricsRegistry _metricsRegistry;

  //Aggregated Server Metrics
  protected final AggregatedTransportServerMetrics _metrics;

  public NettyServer(int port, RequestHandlerFactory handlerFactory, AggregatedMetricsRegistry registry) {
    _port = port;
    _handlerFactory = handlerFactory;
    _metricsRegistry = registry;
    _metrics = new AggregatedTransportServerMetrics(_metricsRegistry, AGGREGATED_SERVER_METRICS_NAME + port + "_");
  }

  @Override
  public void run() {
    try {
      ServerBootstrap bootstrap = getServerBootstrap();

      LOGGER.info("Binding to the server port !!");

      // Bind and start to accept incoming connections.
      ChannelFuture f = bootstrap.bind(_port).sync();
      _channel = f.channel();
      LOGGER.info("Server bounded to port :" + _port + ", Waiting for closing");
      f.channel().closeFuture().sync();
      LOGGER.info("Server boss channel is closed. Gracefully shutting down the server netty threads and pipelines");
    } catch (Exception e) {
      LOGGER.error("Got exception in the main server thread. Stopping !!", e);
    } finally {
      _shutdownComplete.set(true);
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
  public void shutdownGracefully() {
    LOGGER.info("Shutdown requested in the server !!");
    if (null != _channel) {
      LOGGER.info("Closing the server channel");
      _channel.close();
      _bossGroup.shutdownGracefully();
      _workerGroup.shutdownGracefully();
    }
  }

 /**
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
  public static class NettyChannelInboundHandler extends ChannelInboundHandlerAdapter implements ChannelFutureListener {
    private final RequestHandler _handler;
    private final NettyServerMetrics _metric;

    //Metrics Related
    private long _lastRequsetSizeInBytes;
    private long _lastResponseSizeInBytes;
    private TimerContext _lastSendResponseLatency;
    private TimerContext _lastProcessingLatency;

    public NettyChannelInboundHandler(RequestHandler handler, NettyServerMetrics metric) {
      _handler = handler;
      _metric = metric;
    }

    public enum State {
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
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      LOGGER.debug("Request received by server !!");
      _state = State.REQUEST_RECEIVED;
      ByteBuf request = (ByteBuf) msg;
      _lastRequsetSizeInBytes = request.readableBytes();

      //Call processing handler
      _lastProcessingLatency = MetricsHelper.startTimer();
      byte[] response = _handler.processRequest(request);
      _lastProcessingLatency.stop();

      // Send Response
      ByteBuf responseBuf = Unpooled.wrappedBuffer(response);
      _lastSendResponseLatency = MetricsHelper.startTimer();
      ChannelFuture f = ctx.writeAndFlush(responseBuf);
      _state = State.RESPONSE_WRITTEN;
      f.addListener(this);
      request.release();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      _state = State.EXCEPTION;
      LOGGER.error("Got exception in the channel handler", cause);
      _metric.addServingStats(0, 0, 0L, true, 0, 0);
      ctx.close();
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      LOGGER.debug("Response has been sent !!");
      _lastSendResponseLatency.stop();
      _metric.addServingStats(_lastRequsetSizeInBytes, _lastResponseSizeInBytes, 1L, false,
          _lastProcessingLatency.getLatencyMs(), _lastSendResponseLatency.getLatencyMs());
      _state = State.RESPONSE_SENT;
    }

    @Override
    public String toString() {
      return "NettyChannelInboundHandler [_handler=" + _handler + ", _metric=" + _metric + ", _lastRequsetSizeInBytes="
          + _lastRequsetSizeInBytes + ", _lastResponseSizeInBytes=" + _lastResponseSizeInBytes
          + ", _lastSendResponseLatency=" + _lastSendResponseLatency + ", _lastProcessingLatency="
          + _lastProcessingLatency + ", _state=" + _state + "]";
    }
  }

  public boolean isShutdownComplete() {
    return _shutdownComplete.get();
  }

}

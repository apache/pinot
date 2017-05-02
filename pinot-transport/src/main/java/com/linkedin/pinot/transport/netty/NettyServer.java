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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.linkedin.pinot.common.metrics.AggregatedMetricsRegistry;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.metrics.MetricsHelper.TimerContext;
import com.linkedin.pinot.transport.metrics.AggregatedTransportServerMetrics;
import com.linkedin.pinot.transport.metrics.NettyServerMetrics;
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
import io.netty.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  public interface RequestHandler {
    /**
     * Callback for Servers to process the request and return the response.
     * The ownership of the request bytebuf resides with the caler (NettyServer).
     * This callback is not expected to call {@link ByteBuf#release()} on request
     * The ownership of the request byteBuf lies with the caller.
     *
     * The implementation MUST not throw any runtime exceptions. In case of errors,
     * the implementation is expected to construct and return an error response.
     * If the implementation throws runtime exceptions, then the underlying connection
     * will be terminated.
     *
     *
     * @param channelHandlerContext
     * @param request Serialized request
     * @return Serialized response
     */
    ListenableFuture<byte[]> processRequest(ChannelHandlerContext channelHandlerContext, ByteBuf request);
  }

  public interface RequestHandlerFactory {

    /**
     * Request Handler Factory. The RequestHandler objects are not expected to be
     * thread-safe. Hence, we need a factory for the Channel Initializer to use for each incoming channel.
     * @return
     */
    /*
     * TODO/atumbde: Our exisiting request handlers are thread-safe. So, we can replace factory
     * with request handler object
     */
    RequestHandler createNewRequestHandler();
  }

  /**
   * Server port
   */
  protected int _port;

  // Flag to indicate if shutdown has been completed
  protected AtomicBoolean _shutdownComplete = new AtomicBoolean(false);

  //TODO: Need configs to control number of threads
  // NOTE/atumbde: With ScheduledRequestHandler, queries are executed asynchronously.
  // So, these netty threads are not blocked. Config is still important
  protected final EventLoopGroup _bossGroup = new NioEventLoopGroup(1);
  protected final EventLoopGroup _workerGroup = new NioEventLoopGroup(20);

  // Netty Channel
  protected volatile Channel _channel = null;

  // Factory for generating request Handlers
  protected RequestHandlerFactory _handlerFactory;

  // Aggregated Metrics Registry
  protected final AggregatedMetricsRegistry _metricsRegistry;

  //Aggregated Server Metrics
  protected final AggregatedTransportServerMetrics _metrics;

  protected final long _defaultLargeQueryLatencyMs;

  public NettyServer(int port, RequestHandlerFactory handlerFactory, AggregatedMetricsRegistry registry, long defaultLargeQueryLatencyMs) {
    _port = port;
    _handlerFactory = handlerFactory;
    _metricsRegistry = registry;
    _metrics = new AggregatedTransportServerMetrics(_metricsRegistry, AGGREGATED_SERVER_METRICS_NAME + port + "_");
    _defaultLargeQueryLatencyMs = defaultLargeQueryLatencyMs;
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

  public boolean isStarted() {
    return _channel != null;
  }

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
   * Blocking call to wait for shutdown completely.
   */
  public void waitForShutdown(long millis) {
    LOGGER.info("Waiting for Shutdown");

    if (_channel != null) {
      LOGGER.info("Closing the server channel");
      long endTime = System.currentTimeMillis() + millis;

      ChannelFuture channelFuture = _channel.close();
      Future<?> bossGroupFuture = _bossGroup.shutdownGracefully();
      Future<?> workerGroupFuture = _workerGroup.shutdownGracefully();

      long currentTime = System.currentTimeMillis();
      if (endTime > currentTime) {
        channelFuture.awaitUninterruptibly(endTime - currentTime, TimeUnit.MILLISECONDS);
      }

      currentTime = System.currentTimeMillis();
      if (endTime > currentTime) {
        bossGroupFuture.awaitUninterruptibly(endTime - currentTime, TimeUnit.MINUTES);
      }

      currentTime = System.currentTimeMillis();
      if (endTime > currentTime) {
        workerGroupFuture.awaitUninterruptibly(endTime - currentTime, TimeUnit.MINUTES);
      }

      Preconditions.checkState(channelFuture.isDone(), "Unable to close the channel in %s ms", millis);
      Preconditions.checkState(bossGroupFuture.isDone(), "Unable to shutdown the boss group in %s ms", millis);
      Preconditions.checkState(workerGroupFuture.isDone(), "Unable to shutdown the worker group in %s ms", millis);
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
  public static class NettyChannelInboundHandler extends ChannelInboundHandlerAdapter {
    private final long _defaultLargeQueryLatencyMs;
    private final RequestHandler _handler;
    private final NettyServerMetrics _metric;

    public NettyChannelInboundHandler(RequestHandler handler, NettyServerMetrics metric, long defaultLargeQueryLatencyMs) {
      _handler = handler;
      _metric = metric;
      _defaultLargeQueryLatencyMs = defaultLargeQueryLatencyMs;
    }

    public NettyChannelInboundHandler(RequestHandler handler, NettyServerMetrics metric) {
      this(handler, metric, 100);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      final long requestStartTime = System.currentTimeMillis();
      LOGGER.debug("Request received by server !!");

      final ByteBuf request = (ByteBuf) msg;
      final long requestSizeInBytes = request.readableBytes();

      //Call processing handler
      final TimerContext requestProcessingLatency = MetricsHelper.startTimer();
      final ChannelHandlerContext requestChannelHandlerContext = ctx;
      ListenableFuture<byte[]> serializedQueryResponse = _handler.processRequest(ctx, request);
      Futures.addCallback(serializedQueryResponse, new FutureCallback<byte[]>() {
        void sendResponse(@Nonnull final byte[] result) {
          requestProcessingLatency.stop();

          // Send Response
          final ByteBuf responseBuf = Unpooled.wrappedBuffer(result);
          final TimerContext responseSendLatency = MetricsHelper.startTimer();
          ChannelFuture f = requestChannelHandlerContext.writeAndFlush(responseBuf);
          f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future)
                throws Exception {
              LOGGER.debug("Response has been sent !!");
              responseSendLatency.stop();
              _metric.addServingStats(requestSizeInBytes, result.length, 1L, false,
                  requestProcessingLatency.getLatencyMs(), responseSendLatency.getLatencyMs());
              long totalQueryTime = System.currentTimeMillis() - requestStartTime;
              if (totalQueryTime > _defaultLargeQueryLatencyMs) {
                LOGGER.info("Slow query: request handler processing time: {}, send response latency: {}, total time to handle request: {}",
                    requestProcessingLatency.getLatencyMs(),
                    responseSendLatency.getLatencyMs(), totalQueryTime);
              }
            }
          });

          // TODO: check if we can release this right after _handler.processRequest returns
          request.release();
        }
        @Override
        public void onSuccess(@Nullable byte[] result) {
          if (result == null) {
            result = new byte[0];
          }
          sendResponse(result);
        }

        @Override
        public void onFailure(Throwable t) {
          LOGGER.error("Request processing returned unhandled exception, error: ", t);
          sendResponse(new byte[0]);
        }
      });

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOGGER.error("Got exception in the channel handler", cause);
      _metric.addServingStats(0, 0, 0L, true, 0, 0);
      ctx.close();
    }


    @Override
    public String toString() {
      return "NettyChannelInboundHandler [_handler=" + _handler + ", _metric=" + _metric + "]";
    }
  }

  public boolean isShutdownComplete() {
    return _shutdownComplete.get();
  }

}

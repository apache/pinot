package org.apache.pinot.core.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handling netty direct memory OOM. In this case there is a great chance that servers are receiving requests from
 * broker concurrently that needs lot of scans on server pusing servers to OOM. We want to close all channels to broker
 * to proactively release the direct memory.
 */
public class DirectOOMServerHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DirectOOMServerHandler.class);
  private static final AtomicBoolean OOM_SHUTTING_DOWN = new AtomicBoolean(false);

  private final ConcurrentHashMap<SocketChannel, Boolean> _allChannels;
  private volatile boolean _silentShutDown = false;
  private ServerSocketChannel _serverSocketChannel;

  public DirectOOMServerHandler(ConcurrentHashMap<SocketChannel, Boolean> allChannels,
      ServerSocketChannel serverSocketChannel) {
    _allChannels = allChannels;
    _serverSocketChannel = serverSocketChannel;
  }

  public void setSilentShutDown() {
    _silentShutDown = true;
  }

  /**
   * Closes and removes all active channels from the map to release direct memory.
   */
  private void closeAllChannels() {
    LOGGER.warn("OOM detected: Closing all channels to release direct memory");
    for (SocketChannel channel : _allChannels.keySet()) {
      try {
        channel.close();
      } catch (Exception e) {
        LOGGER.error("Error while closing channel: {}", channel, e);
      } finally {
        _allChannels.remove(channel);
      }
    }
  }

  void setSilentShutdown() {
    if (_serverSocketChannel != null) {
      DirectOOMServerHandler directOOMHandler = _serverSocketChannel.pipeline().get(DirectOOMServerHandler.class);
      if (directOOMHandler != null) {
        directOOMHandler.setSilentShutDown();
      }
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    // if we are shutting down channels due to direct memory OOM, we short circuit the channel inactive
    if (_silentShutDown) {
      return;
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (cause instanceof OutOfMemoryError && cause.getMessage().contains("direct buffer")) {
      ServerMetrics.get().addMeteredGlobalValue(ServerMeter.DIRECT_MEMORY_OOM, 1L);
      // Only one thread should handle OOM
      if (OOM_SHUTTING_DOWN.compareAndSet(false, true)) {
        try {
          LOGGER.error("Direct memory OOM detected. Closing all channels to free up memory.", cause);
          closeAllChannels();
          setSilentShutdown();
        } catch (Exception e) {
          LOGGER.error("Error while handling direct memory OOM", e);
        } finally {
          OOM_SHUTTING_DOWN.set(false);
        }
      } else {
        LOGGER.warn("Direct memory OOM detected, but another thread is already handling it.", cause);
      }
    } else {
      // Pass the exception to the next handler in the pipeline
      ctx.fireExceptionCaught(cause);
    }
  }
}

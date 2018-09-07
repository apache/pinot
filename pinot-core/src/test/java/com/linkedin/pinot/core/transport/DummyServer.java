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
package com.linkedin.pinot.core.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import java.util.concurrent.TimeUnit;


/**
 * The {@code DummyServer} class is a Netty server that always responds with the given bytes and the given delay.
 */
public class DummyServer implements Runnable {
  private final int _port;
  private final long _responseDelayMs;
  private final byte[] _responseBytes;

  private volatile Channel _channel;

  public DummyServer(int port, long responseDelayMs, byte[] responseBytes) {
    _port = port;
    _responseDelayMs = responseDelayMs;
    _responseBytes = responseBytes;
  }

  @Override
  public void run() {
    EventLoopGroup bossGroup = new NioEventLoopGroup();
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    try {
      ServerBootstrap serverBootstrap = new ServerBootstrap();
      _channel = serverBootstrap.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, 128)
          .childOption(ChannelOption.SO_KEEPALIVE, true)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              ch.pipeline()
                  .addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Integer.BYTES, 0, Integer.BYTES),
                      new LengthFieldPrepender(Integer.BYTES), new SimpleChannelInboundHandler<ByteBuf>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                          Thread.sleep(_responseDelayMs);
                          ctx.writeAndFlush(ctx.alloc().buffer(_responseBytes.length).writeBytes(_responseBytes),
                              ctx.voidPromise());
                        }
                      });
            }
          })
          .bind(_port)
          .sync()
          .channel();
      _channel.closeFuture().sync();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      // Shut down immediately
      workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
      bossGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
    }
  }

  public boolean isReady() {
    return _channel != null;
  }

  public void shutDown() {
    if (_channel != null) {
      _channel.close();
      _channel = null;
    }
  }
}

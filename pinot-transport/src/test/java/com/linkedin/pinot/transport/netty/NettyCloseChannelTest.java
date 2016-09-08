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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.netty.NettyClientConnection.ResponseFuture;
import com.linkedin.pinot.transport.netty.NettyServer.NettyChannelInboundHandler;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandler;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;
import com.linkedin.pinot.transport.netty.NettyTCPServer.ServerChannelInitializer;


public class NettyCloseChannelTest {

  protected static Logger LOGGER = LoggerFactory.getLogger(NettyCloseChannelTest.class);

  @Test
  /**
   * Client sends a request. Before Server generates the response, the client closes the channel (scenario:as the client is shutting down)
   */
  public void testCloseClientChannel() throws Exception {
    NettyClientMetrics metric = new NettyClientMetrics(null, "abc");
    Timer timer = new HashedWheelTimer();
    String response = "dummy response";
    int port = 9089;
    CountDownLatch latch = new CountDownLatch(1);
    MyRequestHandler handler = new MyRequestHandler(response, latch);
    MyRequestHandlerFactory handlerFactory = new MyRequestHandlerFactory(handler);
    NettyTCPServer serverConn = new NettyTCPServer(port, handlerFactory, null);
    Thread serverThread = new Thread(serverConn, "ServerMain");
    serverThread.start();
    Thread.sleep(1000);
    ServerInstance server = new ServerInstance("localhost", port);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn = new NettyTCPClientConnection(server, eventLoopGroup, timer, metric);
    LOGGER.debug("About to connect the client !!");
    boolean connected = clientConn.connect();
    LOGGER.debug("Client connected !!");
    Assert.assertTrue(connected, "connected");
    Thread.sleep(1000);
    String request = "dummy request";
    LOGGER.debug("Sending the request !!");
    ResponseFuture serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()), 1L, 5000L);
    //Close the client
    clientConn.close();
    latch.countDown();
    ByteBuf serverResp = serverRespFuture.getOne();
    clientConn.close();
    serverConn.shutdownGracefully();
    Assert.assertNull(serverResp);
    Assert.assertFalse(serverRespFuture.isCancelled(), "Is Cancelled");
    Assert.assertTrue(serverRespFuture.getError() != null, "Got Exception");
    serverConn.shutdownGracefully();
    LOGGER.trace("metrics: {}", metric);
  }

  @Test
  /**
   * Client sends a request. Server closes the channel ( scenario: as it is shutting down)
   */
  public void testCloseServerChannel() throws Exception {
    NettyClientMetrics metric = new NettyClientMetrics(null, "abc");
    Timer timer = new HashedWheelTimer();
    int port = 9089;
    MyRequestHandler handler = new MyRequestHandler("empty", null);
    MyRequestHandlerFactory handlerFactory = new MyRequestHandlerFactory(handler);
    NettyTCPServer serverConn = new NettyCloseTCPServer(port, handlerFactory);
    Thread serverThread = new Thread(serverConn, "ServerMain");
    serverThread.start();
    Thread.sleep(1000);
    ServerInstance server = new ServerInstance("localhost", port);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn = new NettyTCPClientConnection(server, eventLoopGroup, timer, metric);
    LOGGER.info("About to connect the client !!");
    boolean connected = clientConn.connect();
    LOGGER.info("Client connected !!");
    Assert.assertTrue(connected, "connected");
    Thread.sleep(1000);
    String request = "dummy request";
    LOGGER.info("Sending the request !!");
    ResponseFuture serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()), 1L, 5000L);
    ByteBuf serverResp = serverRespFuture.getOne();
    clientConn.close();
    serverConn.shutdownGracefully();
    Assert.assertNull(serverResp);
    Assert.assertFalse(serverRespFuture.isCancelled(), "Is Cancelled");
    Assert.assertTrue(serverRespFuture.getError() != null, "Got Exception");
    LOGGER.trace("metrics: {}", metric);
  }

  /**
   * Sets a Handler which closes the connection on incoming request
   */
  public static class NettyCloseTCPServer extends NettyTCPServer {

    public NettyCloseTCPServer(int port, RequestHandlerFactory handlerFactory) {
      super(port, handlerFactory, null);

    }

    @Override
    protected ChannelInitializer<SocketChannel> createChannelInitializer() {
      return new MyServerChannelInitializer(_handlerFactory);
    }
  }

  /**
   * Close the channel on incoming request
   */
  public static class MyNettyChannelInboundHandler extends NettyChannelInboundHandler implements ChannelFutureListener {

    public MyNettyChannelInboundHandler() {
      super(null, null);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      LOGGER.debug("Closing the channel !!");
      ChannelFuture f = ctx.close();
      f.addListener(this);
      LOGGER.debug("Channel close future added !!");
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      LOGGER.debug("Channel is closed !!");
    }
  }

  /**
   * Server Channel Initializer
   */
  protected static class MyServerChannelInitializer extends ServerChannelInitializer {

    public MyServerChannelInitializer(RequestHandlerFactory handlerFactory) {
      super(handlerFactory, null, null);
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      super.initChannel(ch);
      ch.pipeline().removeLast();
      ch.pipeline().addLast("request_handler", new MyNettyChannelInboundHandler());
      LOGGER.debug("Server Channel pipeline setup modified. Pipeline: {}", ch.pipeline().names());

    }
  }

  private static class MyRequestHandler implements RequestHandler {
    private String _response;
    private String _request;
    private final CountDownLatch _responseHandlingLatch;

    public MyRequestHandler(String response, CountDownLatch responseHandlingLatch) {
      _response = response;
      _responseHandlingLatch = responseHandlingLatch;
    }

    @Override
    public ListenableFuture<byte[]> processRequest(ChannelHandlerContext channelHandlerContext, ByteBuf request) {
      byte[] b = new byte[request.readableBytes()];
      request.readBytes(b);
      if (null != _responseHandlingLatch) {
        try {
          _responseHandlingLatch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      _request = new String(b);

      //LOG.info("Server got the request (" + _request + ")");
      return Futures.immediateFuture(_response.getBytes());
    }

    public String getRequest() {
      return _request;
    }

    public String getResponse() {
      return _response;
    }

    public void setResponse(String response) {
      _response = response;
    }
  }

  private static class MyRequestHandlerFactory implements RequestHandlerFactory {
    private final MyRequestHandler _requestHandler;

    public MyRequestHandlerFactory(MyRequestHandler requestHandler) {
      _requestHandler = requestHandler;
    }

    @Override
    public RequestHandler createNewRequestHandler() {
      return _requestHandler;
    }

  }

}

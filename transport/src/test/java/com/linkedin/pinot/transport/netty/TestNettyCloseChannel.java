package com.linkedin.pinot.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.transport.common.ServerInstance;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.netty.NettyClientConnection.ResponseFuture;
import com.linkedin.pinot.transport.netty.NettyServer.NettyChannelInboundHandler;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandler;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;
import com.linkedin.pinot.transport.netty.NettyTCPServer.ServerChannelInitializer;

public class TestNettyCloseChannel {

  protected static Logger LOG = LoggerFactory.getLogger(TestNettyCloseChannel.class);

  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
  }

  @Test
  /**
   * Client sends a request. Before Server generates the response, the client closes the channel (scenario:as the client is shutting down)
   */
  public void testCloseClientChannel() throws Exception
  {
    NettyClientMetrics metric = new NettyClientMetrics(null, "abc");
    String response = "dummy response";
    int port = 9089;
    CountDownLatch latch = new CountDownLatch(1);
    MyRequestHandler handler = new MyRequestHandler(response,latch);
    MyRequestHandlerFactory handlerFactory = new MyRequestHandlerFactory(handler);
    NettyTCPServer serverConn = new NettyTCPServer(port, handlerFactory, null);
    Thread serverThread = new Thread(serverConn,"ServerMain");
    serverThread.start();
    Thread.sleep(1000);
    ServerInstance server = new ServerInstance("localhost", port);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn = new NettyTCPClientConnection(server, eventLoopGroup, metric);
    LOG.info("About to connect the client !!");
    boolean connected = clientConn.connect();
    LOG.info("Client connected !!");
    Assert.assertTrue("connected",connected);
    Thread.sleep(1000);
    String request = "dummy request";
    LOG.info("Sending the request !!");
    ResponseFuture serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()));
    //Close the client
    clientConn.close();
    latch.countDown();
    ByteBuf serverResp = serverRespFuture.getOneResponse();
    clientConn.close();
    serverConn.shutdownGracefully();
    Assert.assertNull(serverResp);
    Assert.assertFalse("Is Cancelled", serverRespFuture.isCancelled());
    Assert.assertTrue("Got Exception", serverRespFuture.getError() != null);
    serverConn.shutdownGracefully();
    System.out.println(metric);
  }

  @Test
  /**
   * Client sends a request. Server closes the channel ( scenario: as it is shutting down)
   */
  public void testCloseServerChannel() throws Exception
  {
    NettyClientMetrics metric = new NettyClientMetrics(null, "abc");
    int port = 9089;
    MyRequestHandler handler = new MyRequestHandler("empty",null);
    MyRequestHandlerFactory handlerFactory = new MyRequestHandlerFactory(handler);
    NettyTCPServer serverConn = new NettyCloseTCPServer(port, handlerFactory);
    Thread serverThread = new Thread(serverConn,"ServerMain");
    serverThread.start();
    Thread.sleep(1000);
    ServerInstance server = new ServerInstance("localhost", port);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn = new NettyTCPClientConnection(server, eventLoopGroup, metric);
    LOG.info("About to connect the client !!");
    boolean connected = clientConn.connect();
    LOG.info("Client connected !!");
    Assert.assertTrue("connected",connected);
    Thread.sleep(1000);
    String request = "dummy request";
    LOG.info("Sending the request !!");
    ResponseFuture serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()));
    ByteBuf serverResp = serverRespFuture.getOneResponse();
    clientConn.close();
    serverConn.shutdownGracefully();
    Assert.assertNull(serverResp);
    Assert.assertFalse("Is Cancelled", serverRespFuture.isCancelled());
    Assert.assertTrue("Got Exception", serverRespFuture.getError() != null);
    System.out.println(metric);
  }

  /**
   * Sets a Handler which closes the connection on incoming request
   */
  public static class NettyCloseTCPServer extends NettyTCPServer
  {

    public NettyCloseTCPServer(int port, RequestHandlerFactory handlerFactory) {
      super(port, handlerFactory, null);

    }

    @Override
    protected ChannelInitializer<SocketChannel> createChannelInitializer()
    {
      return new MyServerChannelInitializer(_handlerFactory);
    }
  }

  /**
   * Close the channel on incoming request
   */
  public static class MyNettyChannelInboundHandler extends NettyChannelInboundHandler implements ChannelFutureListener
  {

    public MyNettyChannelInboundHandler() {
      super(null, null);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
      LOG.info("Closing the channel !!");
      ChannelFuture f = ctx.close();
      f.addListener(this);
      LOG.info("Channel close future added !!");
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      LOG.info("Channel is closed !!");
    }  }

  /**
   * Server Channel Initializer
   */
  protected static class MyServerChannelInitializer
  extends ServerChannelInitializer
  {

    public MyServerChannelInitializer(RequestHandlerFactory handlerFactory) {
      super(handlerFactory, null, null);
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      super.initChannel(ch);
      ch.pipeline().removeLast();
      ch.pipeline().addLast("request_handler", new MyNettyChannelInboundHandler());
      LOG.info("Server Channel pipeline setup modified. Pipeline:" + ch.pipeline().names());

    }
  }

  private static class MyRequestHandler implements RequestHandler
  {
    private String _response;
    private String _request;
    private final CountDownLatch _responseHandlingLatch;

    public MyRequestHandler(String response, CountDownLatch responseHandlingLatch)
    {
      _response = response;
      _responseHandlingLatch = responseHandlingLatch;
    }

    @Override
    public byte[] processRequest(ByteBuf request) {
      byte[] b = new byte[request.readableBytes()];
      request.readBytes(b);
      if ( null != _responseHandlingLatch)
      {
        try {
          _responseHandlingLatch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      _request = new String(b);

      //LOG.info("Server got the request (" + _request + ")");
      return _response.getBytes();
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

  private static class MyRequestHandlerFactory implements RequestHandlerFactory
  {
    private final MyRequestHandler _requestHandler;

    public MyRequestHandlerFactory(MyRequestHandler requestHandler)
    {
      _requestHandler = requestHandler;
    }
    @Override
    public RequestHandler createNewRequestHandler() {
      return _requestHandler;
    }
  }

}

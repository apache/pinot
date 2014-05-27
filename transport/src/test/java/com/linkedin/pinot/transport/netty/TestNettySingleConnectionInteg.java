package com.linkedin.pinot.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.linkedin.pinot.transport.common.ServerInstance;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandler;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;

public class TestNettySingleConnectionInteg {

  protected static Logger LOG = LoggerFactory.getLogger(TestNettySingleConnectionInteg.class);

  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
  }

  @Test
  /**
   * Test Single small request response
   * @throws Exception
   */
  public void testSingleSmallRequestResponse() throws Exception
  {
    String response = "dummy response";
    int port = 9089;
    MyRequestHandler handler = new MyRequestHandler(response, null);
    MyRequestHandlerFactory handlerFactory = new MyRequestHandlerFactory(handler);
    NettyTCPServer serverConn = new NettyTCPServer(port, handlerFactory);
    Thread serverThread = new Thread(serverConn,"ServerMain");
    serverThread.start();
    Thread.sleep(1000);
    ServerInstance server = new ServerInstance("localhost", port);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn = new NettyTCPClientConnection(server, eventLoopGroup);
    LOG.info("About to connect the client !!");
    boolean connected = clientConn.connect();
    LOG.info("Client connected !!");
    Assert.assertTrue("connected",connected);
    Thread.sleep(1000);
    String request = "dummy request";
    LOG.info("Sending the request !!");
    ListenableFuture<ByteBuf> serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()));
    LOG.info("Request  sent !!");
    ByteBuf serverResp = serverRespFuture.get();
    byte[] b2 = new byte[serverResp.readableBytes()];
    serverResp.readBytes(b2);
    String gotResponse = new String(b2);
    Assert.assertEquals("Response Check at client", response, gotResponse);
    Assert.assertEquals("Request Check at server", request, handler.getRequest());
    clientConn.close();
    serverConn.shutdownGracefully();
  }

  @Test
  public void testCancelOutstandingRequest() throws Exception
  {
    String response = "dummy response";
    int port = 9089;
    CountDownLatch latch = new CountDownLatch(1);
    MyRequestHandler handler = new MyRequestHandler(response,latch);
    MyRequestHandlerFactory handlerFactory = new MyRequestHandlerFactory(handler);
    NettyTCPServer serverConn = new NettyTCPServer(port, handlerFactory);
    Thread serverThread = new Thread(serverConn,"ServerMain");
    serverThread.start();
    Thread.sleep(1000);
    ServerInstance server = new ServerInstance("localhost", port);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn = new NettyTCPClientConnection(server, eventLoopGroup);
    LOG.info("About to connect the client !!");
    boolean connected = clientConn.connect();
    LOG.info("Client connected !!");
    Assert.assertTrue("connected",connected);
    Thread.sleep(1000);
    String request = "dummy request";
    LOG.info("Sending the request !!");
    ListenableFuture<ByteBuf> serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()));
    serverRespFuture.cancel(false);
    latch.countDown();
    ByteBuf serverResp = serverRespFuture.get();
    Assert.assertNull(serverResp);
    Assert.assertTrue("Is Cancelled", serverRespFuture.isCancelled());
    clientConn.close();
    serverConn.shutdownGracefully();
  }

  @Test
  public void testConcurrentRequestDispatchError() throws Exception
  {
    String response = "dummy response";
    int port = 9089;
    CountDownLatch latch = new CountDownLatch(1);
    MyRequestHandler handler = new MyRequestHandler(response,latch);
    MyRequestHandlerFactory handlerFactory = new MyRequestHandlerFactory(handler);
    NettyTCPServer serverConn = new NettyTCPServer(port, handlerFactory);
    Thread serverThread = new Thread(serverConn,"ServerMain");
    serverThread.start();
    Thread.sleep(1000);
    ServerInstance server = new ServerInstance("localhost", port);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn = new NettyTCPClientConnection(server, eventLoopGroup);
    LOG.info("About to connect the client !!");
    boolean connected = clientConn.connect();
    LOG.info("Client connected !!");
    Assert.assertTrue("connected",connected);
    Thread.sleep(1000);
    String request = "dummy request";
    LOG.info("Sending the request !!");
    ListenableFuture<ByteBuf> serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()));
    boolean gotException = false;
    try
    {
      clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()));
    } catch(IllegalStateException ex ) {
      gotException = true;
      // Second request should have failed.
      LOG.info("got exception ", ex);
    }
    latch.countDown();
    ByteBuf serverResp = serverRespFuture.get();
    byte[] b2 = new byte[serverResp.readableBytes()];
    serverResp.readBytes(b2);
    String gotResponse = new String(b2);
    Assert.assertEquals("Response Check at client", response, gotResponse);
    Assert.assertEquals("Request Check at server", request, handler.getRequest());
    clientConn.close();
    serverConn.shutdownGracefully();
    Assert.assertTrue("GotException ", gotException);
  }

  private String generatePayload(String prefix, int numBytes)
  {
    StringBuilder b = new StringBuilder(prefix.length() + numBytes);
    b.append(prefix);
    for ( int  i = 0 ; i < numBytes; i++)
    {
      b.append('i');
    }
    return b.toString();
  }

  @Test
  /**
   * Test Single Large  ( 2 MB) request response
   * @throws Exception
   */
  public void testSingleLargeRequestResponse() throws Exception
  {
    String response_prefix = "response_";
    String response = generatePayload(response_prefix, 1024*1024*2);
    int port = 9089;
    MyRequestHandler handler = new MyRequestHandler(response,null);
    MyRequestHandlerFactory handlerFactory = new MyRequestHandlerFactory(handler);
    NettyTCPServer serverConn = new NettyTCPServer(port, handlerFactory);
    Thread serverThread = new Thread(serverConn,"ServerMain");
    serverThread.start();
    Thread.sleep(1000);
    ServerInstance server = new ServerInstance("localhost", port);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn = new NettyTCPClientConnection(server, eventLoopGroup);
    LOG.info("About to connect the client !!");
    boolean connected = clientConn.connect();
    LOG.info("Client connected !!");
    Assert.assertTrue("connected",connected);
    Thread.sleep(1000);
    String request_prefix = "request_";
    String request = generatePayload(request_prefix, 1024*1024*2);
    LOG.info("Sending the request !!");
    ListenableFuture<ByteBuf> serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()));
    LOG.info("Request  sent !!");
    ByteBuf serverResp = serverRespFuture.get();
    byte[] b2 = new byte[serverResp.readableBytes()];
    serverResp.readBytes(b2);
    String gotResponse = new String(b2);
    Assert.assertEquals("Response Check at client", response, gotResponse);
    Assert.assertEquals("Request Check at server", request, handler.getRequest());
    clientConn.close();
    serverConn.shutdownGracefully();
  }

  @Test
  /**
   * Send 10K small sized request in sequence. Verify each request and response.
   * @throws Exception
   */
  public void test10KSmallRequestResponses() throws Exception
  {
    int port = 9089;
    MyRequestHandler handler = new MyRequestHandler(null,null);
    MyRequestHandlerFactory handlerFactory = new MyRequestHandlerFactory(handler);
    NettyTCPServer serverConn = new NettyTCPServer(port, handlerFactory);
    Thread serverThread = new Thread(serverConn,"ServerMain");
    serverThread.start();
    Thread.sleep(1000);
    ServerInstance server = new ServerInstance("localhost", port);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn = new NettyTCPClientConnection(server, eventLoopGroup);
    LOG.info("About to connect the client !!");
    boolean connected = clientConn.connect();
    LOG.info("Client connected !!");
    Assert.assertTrue("connected",connected);
    Thread.sleep(1000);
    for ( int i = 0; i < 10000; i++)
    {
      String request = "dummy request :" + i;
      String response = "dummy response :" + i;
      handler.setResponse(response);
      LOG.info("Sending the request (" + request + ")");
      ListenableFuture<ByteBuf> serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()));
      LOG.info("Request  sent !!");
      ByteBuf serverResp = serverRespFuture.get();
      byte[] b2 = new byte[serverResp.readableBytes()];
      serverResp.readBytes(b2);
      String gotResponse = new String(b2);
      Assert.assertEquals("Response Check at client", response, gotResponse);
      Assert.assertEquals("Request Check at server", request, handler.getRequest());
    }
    clientConn.close();
    serverConn.shutdownGracefully();
  }

  @Test
  /**
   * Send 100 large ( 2MB) sized request in sequence. Verify each request and response.
   * @throws Exception
   */
  public void test100LargeRequestResponses() throws Exception
  {
    int port = 9089;
    MyRequestHandler handler = new MyRequestHandler(null,null);
    MyRequestHandlerFactory handlerFactory = new MyRequestHandlerFactory(handler);
    NettyTCPServer serverConn = new NettyTCPServer(port, handlerFactory);
    Thread serverThread = new Thread(serverConn,"ServerMain");
    serverThread.start();
    Thread.sleep(1000);
    ServerInstance server = new ServerInstance("localhost", port);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn = new NettyTCPClientConnection(server, eventLoopGroup);
    LOG.info("About to connect the client !!");
    boolean connected = clientConn.connect();
    LOG.info("Client connected !!");
    Assert.assertTrue("connected",connected);
    Thread.sleep(1000);
    for ( int i = 0; i < 100; i++)
    {
      String request_prefix = "request_";
      String request = generatePayload(request_prefix, 1024*1024*20);
      String response_prefix = "response_";
      String response = generatePayload(response_prefix, 1024*1024*20);
      handler.setResponse(response);
      //LOG.info("Sending the request (" + request + ")");
      ListenableFuture<ByteBuf> serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()));
      //LOG.info("Request  sent !!");
      ByteBuf serverResp = serverRespFuture.get();
      byte[] b2 = new byte[serverResp.readableBytes()];
      serverResp.readBytes(b2);
      String gotResponse = new String(b2);
      Assert.assertEquals("Response Check at client", response, gotResponse);
      Assert.assertEquals("Request Check at server", request, handler.getRequest());
    }
    clientConn.close();
    serverConn.shutdownGracefully();
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


}

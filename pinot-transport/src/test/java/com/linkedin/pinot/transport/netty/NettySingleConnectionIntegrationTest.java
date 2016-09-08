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
import io.netty.channel.ChannelHandlerContext;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.AsyncResponseFuture;
import com.linkedin.pinot.transport.common.Callback;
import com.linkedin.pinot.transport.common.Cancellable;
import com.linkedin.pinot.transport.common.KeyedFuture;
import com.linkedin.pinot.transport.common.LinkedDequeue;
import com.linkedin.pinot.transport.common.NoneType;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.metrics.PoolStats;
import com.linkedin.pinot.transport.netty.NettyClientConnection.ResponseFuture;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandler;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;
import com.linkedin.pinot.transport.pool.AsyncPool;
import com.linkedin.pinot.transport.pool.AsyncPoolImpl;
import com.linkedin.pinot.transport.pool.AsyncPoolResourceManagerAdapter;
import com.linkedin.pinot.transport.pool.KeyedPool;
import com.linkedin.pinot.transport.pool.KeyedPoolImpl;
import com.yammer.metrics.core.MetricsRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;


public class NettySingleConnectionIntegrationTest {

  protected static Logger LOGGER = LoggerFactory.getLogger(NettySingleConnectionIntegrationTest.class);

  @Test
  /**
   * Test Single small request response
   * @throws Exception
   */
  public void testSingleSmallRequestResponse() throws Exception {
    NettyClientMetrics metric = new NettyClientMetrics(null, "abc");
    Timer timer = new HashedWheelTimer();

    MyServer server = new MyServer();
    Thread.sleep(1000);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn = new NettyTCPClientConnection(server.getServerInstance(), eventLoopGroup, timer, metric);
    try {
      LOGGER.info("About to connect the client !!");
      boolean connected = clientConn.connect();
      LOGGER.info("Client connected !!");
      Assert.assertTrue(connected, "connected");
      Thread.sleep(1000);
      String request = "dummy request";
      LOGGER.info("Sending the request !!");
      ResponseFuture serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()), 1L, 5000L);
      LOGGER.info("Request  sent !!");
      ByteBuf serverResp = serverRespFuture.getOne();
      byte[] b2 = new byte[serverResp.readableBytes()];
      serverResp.readBytes(b2);
      String gotResponse = new String(b2);
      Assert.assertEquals(gotResponse, server.getResponseStr(), "Response Check at client");
      Assert.assertEquals(server.getHandler().getRequest(), request, "Request Check at server");
      System.out.println(metric);
    } finally {
      if (null != clientConn) {
        clientConn.close();
      }

      server.shutdown();
    }
  }

  /*
   * WARNING: This test has potential failures due to timing.
   */
  @Test
  public void testValidatePool() throws Exception {
    NettyClientMetrics metric = new NettyClientMetrics(null, "abc");
    Timer timer = new HashedWheelTimer();

    MyServer server = new MyServer();
    Thread.sleep(1000);
    final String serverName = "SomeServer"; // used as a key to pool. Can be anything.
    ServerInstance serverInstance = server.getServerInstance();
    MetricsRegistry metricsRegistry = new MetricsRegistry();

    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    PooledNettyClientResourceManager resourceManager = new PooledNettyClientResourceManager(eventLoopGroup, new HashedWheelTimer(), metric);
    ExecutorService executorService = Executors.newCachedThreadPool();
    ScheduledExecutorService timeoutExecutor = new ScheduledThreadPoolExecutor(5);
    AsyncPoolResourceManagerAdapter<ServerInstance, NettyClientConnection> rmAdapter =
        new AsyncPoolResourceManagerAdapter<ServerInstance, NettyClientConnection>(serverInstance, resourceManager, executorService, metricsRegistry);
    AsyncPool pool = new AsyncPoolImpl<NettyClientConnection>(serverName, rmAdapter,
     /*maxSize=*/5, /*idleTimeoutMs=*/100000, timeoutExecutor,
        executorService, /*maxWaiters=*/10, AsyncPoolImpl.Strategy.LRU, /*minSize=*/2, metricsRegistry);
    pool.start();

    Callback<NoneType> callback;

    callback = new Callback<NoneType>() {
      @Override
      public void onSuccess(NoneType arg0) {
      }

      @Override
      public void onError(Throwable arg0) {
        Assert.fail("Shutdown error");
      }
    };


    boolean serverShutdown = false;
    try {
      PoolStats stats;
      /* Validate with no connection in pool */
      Thread.sleep(3000);   // Give the pool enough time to create connections (in this case, 2 connections minSize)
      System.out.println("Validating with no used objects in the pool");
      pool.validate(false);
      stats = pool.getStats(); // System.out.println(stats);
      Assert.assertEquals(2, stats.getPoolSize());
      Assert.assertEquals(0, stats.getTotalBadDestroyed());
      /* checkout one connection, it should not destroy anything */
      AsyncResponseFuture<ServerInstance, NettyClientConnection> future = new AsyncResponseFuture<ServerInstance, NettyClientConnection>(serverInstance, "Future for " + serverName);
      Cancellable cancellable = pool.get(future);
      future.setCancellable(cancellable);
      NettyClientConnection conn = future.getOne();
      stats = pool.getStats(); // System.out.println(stats);
      System.out.println("Validating with one used object in the pool");
      pool.validate(false);
      Assert.assertEquals(2, stats.getPoolSize());
      Assert.assertEquals(0, stats.getTotalBadDestroyed());
      Assert.assertEquals(1, stats.getCheckedOut());

      // Now stop the server, so that the checked out connection is invalidated.
      server.shutdown();
      serverShutdown = true;;
      Thread.sleep(2000); // Wait for the client channel to be closed.
      pool.validate(false);
      Thread.sleep(5000); // Wait for the callback into AsyncPoolImpl after the destroy thread completes destroying the connection
      System.out.println("Validating with one used object in the pool, after server shutdown");
      stats = pool.getStats(); // System.out.println(stats);
      Assert.assertEquals(2, stats.getPoolSize());
      Assert.assertEquals(1, stats.getTotalBadDestroyed());
      Assert.assertEquals(1, stats.getCheckedOut());


    } finally {
      server.shutdown();
      pool.shutdown(callback);
      executorService.shutdown();
      timeoutExecutor.shutdown();
    }
  }

  @Test
  public void testCancelOutstandingRequest() throws Exception {
    NettyClientMetrics metric = new NettyClientMetrics(null, "abc");
    CountDownLatch latch = new CountDownLatch(1);
    MyServer server = new MyServer();
    Thread.sleep(1000);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn =
        new NettyTCPClientConnection(server.getServerInstance(), eventLoopGroup, new HashedWheelTimer(), metric);
    LOGGER.info("About to connect the client !!");
    boolean connected = clientConn.connect();
    LOGGER.info("Client connected !!");
    Assert.assertTrue(connected, "connected");
    Thread.sleep(1000);
    String request = "dummy request";
    LOGGER.info("Sending the request !!");
    ResponseFuture serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()), 1L, 5000L);
    serverRespFuture.cancel(false);
    latch.countDown();
    ByteBuf serverResp = serverRespFuture.getOne();
    Assert.assertNull(serverResp);
    Assert.assertTrue(serverRespFuture.isCancelled(), "Is Cancelled");
    clientConn.close();
    server.shutdown();
  }

  @Test
  public void testConcurrentRequestDispatchError() throws Exception {
    NettyClientMetrics metric = new NettyClientMetrics(null, "abc");
    CountDownLatch latch = new CountDownLatch(1);
    MyServer server = new MyServer();
    Thread.sleep(1000);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn =
        new NettyTCPClientConnection(server.getServerInstance(), eventLoopGroup, new HashedWheelTimer(), metric);
    LOGGER.info("About to connect the client !!");
    boolean connected = clientConn.connect();
    LOGGER.info("Client connected !!");
    Assert.assertTrue(connected, "connected");
    Thread.sleep(1000);
    String request = "dummy request";
    LOGGER.info("Sending the request !!");
    ResponseFuture serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()), 1L, 5000L);
    boolean gotException = false;
    try {
      clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()), 1L, 5000L);
    } catch (IllegalStateException ex) {
      gotException = true;
      // Second request should have failed.
      LOGGER.info("got exception ", ex);
    }
    latch.countDown();
    ByteBuf serverResp = serverRespFuture.getOne();
    byte[] b2 = new byte[serverResp.readableBytes()];
    serverResp.readBytes(b2);
    String gotResponse = new String(b2);
    Assert.assertEquals(gotResponse, server.getResponseStr(), "Response Check at client");
    Assert.assertEquals(server.getHandler().getRequest(), request, "Request Check at server");
    clientConn.close();
    server.shutdown();
    Assert.assertTrue(gotException, "GotException ");
  }

  private String generatePayload(String prefix, int numBytes) {
    StringBuilder b = new StringBuilder(prefix.length() + numBytes);
    b.append(prefix);
    for (int i = 0; i < numBytes; i++) {
      b.append('i');
    }
    return b.toString();
  }

  @Test
  /**
   * Test Single Large  ( 2 MB) request response
   * @throws Exception
   */
  public void testSingleLargeRequestResponse() throws Exception {
    NettyClientMetrics metric = new NettyClientMetrics(null, "abc");
    final String response_prefix = "response_";
    final String response = generatePayload(response_prefix, 1024 * 1024 * 2);
    MyServer server = new MyServer(response);
    Thread.sleep(1000);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn =
        new NettyTCPClientConnection(server.getServerInstance(), eventLoopGroup, new HashedWheelTimer(), metric);
    try {
      LOGGER.info("About to connect the client !!");
      boolean connected = clientConn.connect();
      LOGGER.info("Client connected !!");
      Assert.assertTrue(connected, "connected");
      Thread.sleep(1000);
      String request_prefix = "request_";
      String request = generatePayload(request_prefix, 1024 * 1024 * 2);
      LOGGER.info("Sending the request !!");
      ResponseFuture serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()), 1L, 5000L);
      LOGGER.info("Request  sent !!");
      ByteBuf serverResp = serverRespFuture.getOne();
      byte[] b2 = new byte[serverResp.readableBytes()];
      serverResp.readBytes(b2);
      String gotResponse = new String(b2);
      Assert.assertEquals(gotResponse, response, "Response Check at client");
      Assert.assertEquals(server.getHandler().getRequest(), request, "Request Check at server");
    } finally {
      if (null != clientConn) {
        clientConn.close();
      }
      server.shutdown();
    }
  }

  @Test
  /**
   * Send 10K small sized request in sequence. Verify each request and response.
   * @throws Exception
   */
  public void test10KSmallRequestResponses() throws Exception {
    NettyClientMetrics metric = new NettyClientMetrics(null, "abc");
    MyServer server = new MyServer(null);
    Thread.sleep(1000);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn =
        new NettyTCPClientConnection(server.getServerInstance(), eventLoopGroup, new HashedWheelTimer(), metric);
    try {
      LOGGER.info("About to connect the client !!");
      boolean connected = clientConn.connect();
      LOGGER.info("Client connected !!");
      Assert.assertTrue(connected, "connected");
      Thread.sleep(1000);
      for (int i = 0; i < 10000; i++) {
        String request = "dummy request :" + i;
        String response = "dummy response :" + i;
        server.getHandler().setResponse(response);
        LOGGER.info("Sending the request (" + request + ")");
        ResponseFuture serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()), 1L, 5000L);
        LOGGER.info("Request  sent !!");
        ByteBuf serverResp = serverRespFuture.getOne();
        if (null == serverResp) {
          LOGGER.error("Got unexpected error while trying to get response.", serverRespFuture.getError());
        }

        byte[] b2 = new byte[serverResp.readableBytes()];
        serverResp.readBytes(b2);
        String gotResponse = new String(b2);
        Assert.assertEquals(gotResponse, response, "Response Check at client");
        Assert.assertEquals(server.getHandler().getRequest(), request, "Request Check at server");
      }
    } finally {
      if (null != clientConn) {
        clientConn.close();
      }

      server.shutdown();
    }
  }

  //@Test
  //@Ignore
  /**
   * Send 100 large ( 2MB) sized request in sequence. Verify each request and response.
   * @throws Exception
   */
  //@Test
  public void test100LargeRequestResponses() throws Exception {
    NettyClientMetrics metric = new NettyClientMetrics(null, "abc");
    MyServer server = new MyServer(null);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyTCPClientConnection clientConn =
        new NettyTCPClientConnection(server.getServerInstance(), eventLoopGroup, new HashedWheelTimer(), metric);
    LOGGER.info("About to connect the client !!");
    boolean connected = clientConn.connect();
    LOGGER.info("Client connected !!");
    Assert.assertTrue(connected, "connected");
    Thread.sleep(1000);
    try {
      for (int i = 0; i < 100; i++) {
        String request_prefix = "request_";
        String request = generatePayload(request_prefix, 1024 * 1024 * 20);
        String response_prefix = "response_";
        String response = generatePayload(response_prefix, 1024 * 1024 * 20);
        server.getHandler().setResponse(response);
        //LOG.info("Sending the request (" + request + ")");
        ResponseFuture serverRespFuture = clientConn.sendRequest(Unpooled.wrappedBuffer(request.getBytes()), 1L, 5000L);
        //LOG.info("Request  sent !!");
        ByteBuf serverResp = serverRespFuture.getOne();
        byte[] b2 = new byte[serverResp.readableBytes()];
        serverResp.readBytes(b2);
        String gotResponse = new String(b2);
        Assert.assertEquals(gotResponse, response, "Response Check at client");
        Assert.assertEquals(server.getHandler().getRequest(), request, "Request Check at server");
      }
    } finally {
      if (null != clientConn) {
        clientConn.close();
      }

      server.shutdown();
    }
  }

  /*
   * This test attempts to use the connection mechanism the same way as ScatterGatherImpl.SingleRequestHandler does.
   *
   * WARNING: This test has potential failures due to timing.
   */
  @Test
  public void testServerShutdownLeak() throws Exception {
    final NettyClientMetrics metric = new NettyClientMetrics(null, "abc");
    final Timer timer = new HashedWheelTimer();
    final int minConns = 2;
    final int maxConns = 3;
    final int maxIdleTimeoutMs = 10000000; // 10M ms.
    final int maxBacklogPerServer = 1;

    MyServer server = new MyServer();
    Thread.sleep(1000);
    final String serverName = "SomeServer"; // used as a key to pool. Can be anything.
    final ServerInstance serverInstance = server.getServerInstance();
    final MetricsRegistry metricsRegistry = new MetricsRegistry();

    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    PooledNettyClientResourceManager resourceManager = new PooledNettyClientResourceManager(eventLoopGroup, new HashedWheelTimer(), metric);
    ExecutorService executorService = Executors.newCachedThreadPool();
    ScheduledExecutorService timeoutExecutor = new ScheduledThreadPoolExecutor(5);

    AsyncPoolResourceManagerAdapter<ServerInstance, NettyClientConnection> rmAdapter =
        new AsyncPoolResourceManagerAdapter<ServerInstance, NettyClientConnection>(serverInstance, resourceManager, executorService, metricsRegistry);
    KeyedPool<ServerInstance, NettyClientConnection> keyedPool = new KeyedPoolImpl<ServerInstance, NettyClientConnection>(minConns,
        maxConns, maxIdleTimeoutMs, maxBacklogPerServer,
        resourceManager, timeoutExecutor, executorService, metricsRegistry);
    resourceManager.setPool(keyedPool);

    keyedPool.start();

    Field keyedPoolMap = KeyedPoolImpl.class.getDeclaredField("_keyedPool");
    keyedPoolMap.setAccessible(true);

    KeyedFuture<ServerInstance, NettyClientConnection> keyedFuture = keyedPool.checkoutObject(serverInstance);
    // The connection pool for this server is created on demand, so we can now get a reference to the _keyedPool.
    // The act of calling checkoutObject() creates a new AsyncPoolImpl and places a request for a new connection.
    // Since no new connections are available in the beginning, we always end up creating one more than the min.

    Map<ServerInstance, AsyncPool<NettyClientConnection>> poolMap = (Map<ServerInstance, AsyncPool<NettyClientConnection>>)keyedPoolMap.get(
        keyedPool);
    AsyncPool<NettyClientConnection> asyncPool = poolMap.get(serverInstance);
    Field waiterList = AsyncPoolImpl.class.getDeclaredField("_waiters");
    waiterList.setAccessible(true);
    LinkedDequeue queue = (LinkedDequeue) waiterList.get(asyncPool);

    PoolStats stats;

    // If the number of waiters is = 0, then we will error out because the min connections may not have completed
    // by the time we check one out. If maxWaiters is > 0, then we may end up initiating a fresh connection while the
    // min is still being filled. So, best to sleep a little to make sure that the min pool size is filled out, so that
    // the stats are correct.
    Thread.sleep(2000L);

    stats =  asyncPool.getStats();
    Assert.assertEquals(stats.getIdleCount(), minConns);
    Assert.assertEquals(stats.getPoolSize(), minConns+1);

    NettyClientConnection conn = keyedFuture.getOne();
    LOGGER.debug("Got connection ID " + conn.getConnId());
    Assert.assertEquals(stats.getIdleCount(), minConns);
    Assert.assertEquals(stats.getPoolSize(), minConns+1);

    // Now get two more connections to the server, since we have 2 idle, we should get those.
    // And leak them.
    keyedFuture = keyedPool.checkoutObject(serverInstance);
    conn = keyedFuture.getOne();
    LOGGER.debug("Got connection ID " + conn.getConnId());
    keyedFuture = keyedPool.checkoutObject(serverInstance);
    conn = keyedFuture.getOne();
    LOGGER.debug("Got connection ID " + conn.getConnId());

    // Now we should have 0 idle, and a pool size of 3 with no waiters.
    stats =  asyncPool.getStats();
    Assert.assertEquals(stats.getIdleCount(), 0);
    Assert.assertEquals(stats.getPoolSize(), minConns+1);
    Assert.assertEquals(queue.size(), 0);

    // Now, we will always get an exception because we don't have a free connection to the server.
    {
      keyedFuture = keyedPool.checkoutObject(serverInstance);
      boolean caughtException = false;
      LOGGER.debug("Will never get a connection here.");
      try {
        conn = keyedFuture.getOne(3, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        caughtException = true;
      }
      Assert.assertTrue(caughtException);
      keyedFuture.cancel(true);
    }
    // Now if the server goes down, we should release all three connections and be able to get a successful new connection
    LOGGER.info("Shutting down server instance");
    server.shutdown();
    Thread.sleep(2000L);  // Give it time to clean up on the client side.
    stats =  asyncPool.getStats();
    LOGGER.debug(stats.toString());
    // There will be a couple in idleCount in error state.
    Assert.assertEquals(stats.getIdleCount(), minConns);
    Assert.assertEquals(stats.getPoolSize(), minConns);
    LOGGER.debug("Restarting server instance");
    server.restart();
    Thread.sleep(3000);
    LOGGER.debug("Server restart successful\n" + asyncPool.getStats());
    // Now get 3 connections successfully
    for (int i = 0; i < 3; i++) {
      keyedFuture = keyedPool.checkoutObject(serverInstance);
      conn = keyedFuture.getOne();
      Assert.assertNotNull(conn);
    }
    server.shutdown();
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

  private static class MyServer {
    private final int _port = 9089;
    private final String _responseStr;
    private MyRequestHandler _handler;
    private NettyTCPServer _serverConn;
    private ServerInstance _serverInstance;
    private boolean _hasShutDown = false;

    public MyServer(String responseStr) {
      _responseStr = responseStr;
      _handler = new MyRequestHandler(_responseStr, null);
      MyRequestHandlerFactory handlerFactory = new MyRequestHandlerFactory(_handler);
      _serverConn = new NettyTCPServer(_port, handlerFactory, null);
      Thread serverThread = new Thread(_serverConn, "ServerMain");
      serverThread.start();
      _serverInstance = new ServerInstance("localhost", _port);
    }
    public MyServer() {
      this("dummy response");
    }

    public MyRequestHandler getHandler() {
      return _handler;
    }
    public NettyTCPServer getServerConn() {
      return _serverConn;
    }
    public ServerInstance getServerInstance() {
      return _serverInstance;
    }
    public final String getResponseStr() {
      return _responseStr;
    }
    public void shutdown() {
      if (!_hasShutDown && _serverConn != null) {
        _serverConn.shutdownGracefully();
        _hasShutDown = true;
      }
    }
    public void restart() {
      _handler = new MyRequestHandler(_responseStr, null);
      MyRequestHandlerFactory handlerFactory = new MyRequestHandlerFactory(_handler);
      _serverConn = new NettyTCPServer(_port, handlerFactory, null);
      Thread serverThread = new Thread(_serverConn, "ServerMain");
      serverThread.start();
      _hasShutDown = false;
    }
  }
}

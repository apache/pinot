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

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.AsyncResponseFuture;
import com.linkedin.pinot.transport.common.Callback;
import com.linkedin.pinot.transport.common.LinkedDequeue;
import com.linkedin.pinot.transport.common.NoneType;
import com.linkedin.pinot.transport.common.ServerResponseFuture;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.metrics.PoolStats;
import com.linkedin.pinot.transport.netty.NettyClientConnection.ResponseFuture;
import com.linkedin.pinot.transport.pool.AsyncPool;
import com.linkedin.pinot.transport.pool.AsyncPoolImpl;
import com.linkedin.pinot.transport.pool.AsyncPoolResourceManagerAdapter;
import com.linkedin.pinot.transport.pool.KeyedPool;
import com.linkedin.pinot.transport.pool.KeyedPoolImpl;
import com.yammer.metrics.core.MetricsRegistry;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class NettySingleConnectionIntegrationTest {
  private static final int NUM_SMALL_TESTS = 1000;
  private static final int NUM_LARGE_TESTS = 10;
  private static final int NUM_LARGE_PAYLOAD_CHARACTERS = 2 * 1000 * 1000;

  private NettyTestUtils.LatchControlledRequestHandler _requestHandler;
  private NettyTCPServer _nettyTCPServer;
  private ServerInstance _clientServer;
  private NettyTCPClientConnection _nettyTCPClientConnection;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _requestHandler = new NettyTestUtils.LatchControlledRequestHandler(null);
    _requestHandler.setResponse(NettyTestUtils.DUMMY_RESPONSE);
    NettyTestUtils.LatchControlledRequestHandlerFactory handlerFactory =
        new NettyTestUtils.LatchControlledRequestHandlerFactory(_requestHandler);
    _nettyTCPServer = new NettyTCPServer(NettyTestUtils.DEFAULT_PORT, handlerFactory, null);
    Thread serverThread = new Thread(_nettyTCPServer, "NettyTCPServer");
    serverThread.start();
    // Wait for at most 10 seconds for server to start
    NettyTestUtils.waitForServerStarted(_nettyTCPServer, 10 * 1000L);

    _clientServer = new ServerInstance("localhost", NettyTestUtils.DEFAULT_PORT);
    _nettyTCPClientConnection =
        new NettyTCPClientConnection(_clientServer, new NioEventLoopGroup(), new HashedWheelTimer(),
            new NettyClientMetrics(null, "abc"));
  }

  @Test
  public void testSmallRequestResponses()
      throws Exception {
    Assert.assertTrue(_nettyTCPClientConnection.connect());
    for (int i = 0; i < NUM_SMALL_TESTS; i++) {
      String request = NettyTestUtils.DUMMY_REQUEST + i;
      String response = NettyTestUtils.DUMMY_RESPONSE + i;
      _requestHandler.setResponse(response);
      ResponseFuture responseFuture =
          _nettyTCPClientConnection.sendRequest(Unpooled.wrappedBuffer(request.getBytes()), 1L, 5000L);
      byte[] bytes = responseFuture.getOne();
      Assert.assertEquals(new String(bytes), response);
      Assert.assertEquals(_requestHandler.getRequest(), request);
    }
  }

  @Test
  public void testLargeRequestResponses()
      throws Exception {
    String payload = RandomStringUtils.random(NUM_LARGE_PAYLOAD_CHARACTERS);
    String requestPayload = NettyTestUtils.DUMMY_REQUEST + payload;
    String responsePayload = NettyTestUtils.DUMMY_RESPONSE + payload;

    Assert.assertTrue(_nettyTCPClientConnection.connect());
    for (int i = 0; i < NUM_LARGE_TESTS; i++) {
      String request = requestPayload + i;
      String response = responsePayload + i;
      _requestHandler.setResponse(response);
      ResponseFuture responseFuture =
          _nettyTCPClientConnection.sendRequest(Unpooled.wrappedBuffer(request.getBytes()), 1L, 5000L);
      byte[] bytes = responseFuture.getOne();
      Assert.assertEquals(new String(bytes), response);
      Assert.assertEquals(_requestHandler.getRequest(), request);
    }
  }

  @Test
  public void testCancelOutstandingRequest()
      throws Exception {
    Assert.assertTrue(_nettyTCPClientConnection.connect());
    ResponseFuture responseFuture =
        _nettyTCPClientConnection.sendRequest(Unpooled.wrappedBuffer(NettyTestUtils.DUMMY_REQUEST.getBytes()), 1L,
            5000L);
    responseFuture.cancel(false);
    // Wait for cancel taking effect
    Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
    Assert.assertNull(responseFuture.getOne());
    Assert.assertTrue(responseFuture.isCancelled());
  }

  @Test
  public void testConcurrentRequestDispatchError()
      throws Exception {
    Assert.assertTrue(_nettyTCPClientConnection.connect());
    ResponseFuture responseFuture =
        _nettyTCPClientConnection.sendRequest(Unpooled.wrappedBuffer(NettyTestUtils.DUMMY_REQUEST.getBytes()), 1L,
            5000L);
    try {
      _nettyTCPClientConnection.sendRequest(Unpooled.wrappedBuffer(NettyTestUtils.DUMMY_REQUEST.getBytes()), 1L, 5000L);
      Assert.fail("Concurrent request should throw IllegalStateException");
    } catch (IllegalStateException e) {
      // Pass
    }
    byte[] bytes = responseFuture.getOne();
    Assert.assertEquals(new String(bytes), NettyTestUtils.DUMMY_RESPONSE);
    Assert.assertEquals(_requestHandler.getRequest(), NettyTestUtils.DUMMY_REQUEST);
  }

  @Test
  public void testValidatePool()
      throws Exception {
    PooledNettyClientResourceManager resourceManager =
        new PooledNettyClientResourceManager(new NioEventLoopGroup(), new HashedWheelTimer(),
            new NettyClientMetrics(null, "abc"));
    ExecutorService executorService = Executors.newCachedThreadPool();
    ScheduledExecutorService timeoutExecutor = new ScheduledThreadPoolExecutor(5);

    String serverName = "server";
    MetricsRegistry metricsRegistry = new MetricsRegistry();
    AsyncPoolResourceManagerAdapter<PooledNettyClientResourceManager.PooledClientConnection> rmAdapter =
        new AsyncPoolResourceManagerAdapter<>(_clientServer, resourceManager, executorService, metricsRegistry);
    AsyncPool<PooledNettyClientResourceManager.PooledClientConnection> pool = new AsyncPoolImpl<>(serverName, rmAdapter,
     /*maxSize=*/5, /*idleTimeoutMs=*/100000L, timeoutExecutor, executorService, /*maxWaiters=*/10,
        AsyncPoolImpl.Strategy.LRU, /*minSize=*/2, metricsRegistry);

    try {
      pool.start();
      Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);

      // Test no connection in pool
      Assert.assertTrue(pool.validate(false));
      PoolStats stats = pool.getStats();
      Assert.assertEquals(stats.getPoolSize(), 2);
      Assert.assertEquals(stats.getTotalBadDestroyed(), 0);
      Assert.assertEquals(stats.getCheckedOut(), 0);

      // Test one connection, it should not destroy anything
      AsyncResponseFuture<PooledNettyClientResourceManager.PooledClientConnection> responseFuture =
          new AsyncResponseFuture<>(_clientServer, null);
      pool.get(responseFuture);
      Assert.assertNotNull(responseFuture.getOne());
      Assert.assertTrue(pool.validate(false));
      stats = pool.getStats();
      Assert.assertEquals(stats.getPoolSize(), 2);
      Assert.assertEquals(stats.getTotalBadDestroyed(), 0);
      Assert.assertEquals(stats.getCheckedOut(), 1);

      // Now stop the server, so that the checked out connection is invalidated
      NettyTestUtils.closeServerConnection(_nettyTCPServer);
      Assert.assertTrue(pool.validate(false));
      stats = pool.getStats();
      Assert.assertEquals(stats.getPoolSize(), 2);
      Assert.assertEquals(stats.getTotalBadDestroyed(), 1);
      Assert.assertEquals(stats.getCheckedOut(), 1);
    } finally {
      pool.shutdown(new Callback<NoneType>() {
        @Override
        public void onSuccess(NoneType arg0) {
        }

        @Override
        public void onError(Throwable arg0) {
          Assert.fail("Shutdown error");
        }
      });
      executorService.shutdown();
      timeoutExecutor.shutdown();
    }
  }

  /**
   * This test attempts to use the connection mechanism the same way as ScatterGatherImpl.SingleRequestHandler does.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testServerShutdownLeak()
      throws Exception {
    PooledNettyClientResourceManager resourceManager =
        new PooledNettyClientResourceManager(new NioEventLoopGroup(), new HashedWheelTimer(),
            new NettyClientMetrics(null, "abc"));
    ExecutorService executorService = Executors.newCachedThreadPool();
    ScheduledExecutorService timeoutExecutor = new ScheduledThreadPoolExecutor(5);

    KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> keyedPool =
        new KeyedPoolImpl<>(/*minSize=*/2, /*maxSize=*/3, /*idleTimeoutMs=*/100000L, /*maxPending=*/1, resourceManager,
            timeoutExecutor, executorService, new MetricsRegistry());
    resourceManager.setPool(keyedPool);

    try {
      keyedPool.start();

      // The act of calling checkoutObject() creates a new AsyncPool and places a request for a new connection
      // NOTE: since no connections are available in the beginning, and the min connections are still being filled, we
      // always end up creating one more connection than the min connections
      Assert.assertNotNull(keyedPool.checkoutObject(_clientServer, "none").getOne());

      // Use reflection to get the pool and the waiters queue
      Field keyedPoolField = KeyedPoolImpl.class.getDeclaredField("_keyedPool");
      keyedPoolField.setAccessible(true);
      Map<ServerInstance, AsyncPool<NettyClientConnection>> poolMap =
          (Map<ServerInstance, AsyncPool<NettyClientConnection>>) keyedPoolField.get(keyedPool);
      AsyncPool<NettyClientConnection> pool = poolMap.get(_clientServer);
      Field waitersField = AsyncPoolImpl.class.getDeclaredField("_waiters");
      waitersField.setAccessible(true);
      LinkedDequeue waitersQueue = (LinkedDequeue) waitersField.get(pool);

      // Make sure the min pool size is filled out
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

      PoolStats stats = pool.getStats();
      Assert.assertEquals(stats.getPoolSize(), 3);
      Assert.assertEquals(stats.getIdleCount(), 2);
      Assert.assertEquals(stats.getCheckedOut(), 1);
      Assert.assertEquals(waitersQueue.size(), 0);

      // Get two more connections to the server and leak them
      Assert.assertNotNull(keyedPool.checkoutObject(_clientServer, "none").getOne());
      Assert.assertNotNull(keyedPool.checkoutObject(_clientServer, "none").getOne());
      stats = pool.getStats();
      Assert.assertEquals(stats.getPoolSize(), 3);
      Assert.assertEquals(stats.getIdleCount(), 0);
      Assert.assertEquals(stats.getCheckedOut(), 3);
      Assert.assertEquals(waitersQueue.size(), 0);

      // Try to get one more connection
      // We should get an exception because we don't have a free connection to the server
      ServerResponseFuture<PooledNettyClientResourceManager.PooledClientConnection> serverResponseFuture =
          keyedPool.checkoutObject(_clientServer, "none");
      try {
        serverResponseFuture.getOne(1, TimeUnit.SECONDS);
        Assert.fail("Get connection even no connections available");
      } catch (TimeoutException e) {
        // PASS
      }
      stats = pool.getStats();
      Assert.assertEquals(stats.getPoolSize(), 3);
      Assert.assertEquals(stats.getIdleCount(), 0);
      Assert.assertEquals(stats.getCheckedOut(), 3);
      Assert.assertEquals(waitersQueue.size(), 1);
      serverResponseFuture.cancel(true);
      Assert.assertEquals(waitersQueue.size(), 0);

      // If the server goes down, we should release all 3 connections and be able to get new connections
      NettyTestUtils.closeServerConnection(_nettyTCPServer);
      setUp();
      stats = pool.getStats();
      Assert.assertEquals(stats.getPoolSize(), 2);
      Assert.assertEquals(stats.getIdleCount(), 2);

      // Try to get 3 new connections
      for (int i = 0; i < 3; i++) {
        Assert.assertNotNull(keyedPool.checkoutObject(_clientServer, "none").getOne());
      }
    } finally {
      keyedPool.shutdown();
      executorService.shutdown();
      timeoutExecutor.shutdown();
    }
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    NettyTestUtils.closeClientConnection(_nettyTCPClientConnection);
    NettyTestUtils.closeServerConnection(_nettyTCPServer);
  }
}

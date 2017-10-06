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
package com.linkedin.pinot.transport.scattergather;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.common.SegmentIdSet;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.netty.NettyServer;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandler;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;
import com.linkedin.pinot.transport.netty.NettyTCPServer;
import com.linkedin.pinot.transport.netty.PooledNettyClientResourceManager;
import com.linkedin.pinot.transport.pool.KeyedPoolImpl;
import com.yammer.metrics.core.MetricsRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.ResourceLeakDetector;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ScatterGatherTest {
  private static final String LOCAL_HOST = "localhost";
  private static final int BASE_SERVER_PORT = 7071;
  private static final int NUM_SERVERS = 4;

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @Test
  public void testNormal() throws Exception {
    NettyServer[] nettyServers = new NettyServer[NUM_SERVERS];
    ServerInstance[] serverInstances = new ServerInstance[NUM_SERVERS];
    Map<ServerInstance, SegmentIdSet> serverToSegmentsMap = new HashMap<>(NUM_SERVERS);
    Map<ServerInstance, String> serverToRequestMap = new HashMap<>(NUM_SERVERS);

    for (int i = 0; i < NUM_SERVERS; i++) {
      int serverPort = BASE_SERVER_PORT + i;
      nettyServers[i] = new NettyTCPServer(serverPort, new TestRequestHandlerFactory(0L, false), null);
      new Thread(nettyServers[i]).start();

      ServerInstance serverInstance = new ServerInstance(LOCAL_HOST, serverPort);
      serverInstances[i] = serverInstance;
      serverToSegmentsMap.put(serverInstance, new SegmentIdSet());
      serverToRequestMap.put(serverInstance, "request_" + i);
    }

    // Setup client
    MetricsRegistry metricsRegistry = new MetricsRegistry();
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    KeyedPoolImpl<PooledNettyClientResourceManager.PooledClientConnection> connectionPool =
        setUpConnectionPool(metricsRegistry, eventLoopGroup);
    ExecutorService executorService = Executors.newCachedThreadPool();
    ScatterGather scatterGather = new ScatterGatherImpl(connectionPool, executorService);
    ScatterGatherStats scatterGatherStats = new ScatterGatherStats();
    BrokerMetrics brokerMetrics = new BrokerMetrics(metricsRegistry);

    // Send the request
    ScatterGatherRequest scatterGatherRequest =
        new TestScatterGatherRequest(serverToSegmentsMap, serverToRequestMap, 10_000L);
    CompositeFuture<ByteBuf> future =
        scatterGather.scatterGather(scatterGatherRequest, scatterGatherStats, brokerMetrics);

    // Should have response from all servers
    Map<ServerInstance, ByteBuf> serverToResponseMap = future.get();
    Assert.assertEquals(serverToResponseMap.size(), NUM_SERVERS);
    for (int i = 0; i < NUM_SERVERS; i++) {
      Assert.assertEquals(getResponse(serverToResponseMap.get(serverInstances[i])),
          serverToRequestMap.get(serverInstances[i]));
    }

    // Should get empty error map
    Map<ServerInstance, Throwable> serverToErrorMap = future.getError();
    Assert.assertTrue(serverToErrorMap.isEmpty());

    connectionPool.shutdown();
    executorService.shutdown();
    eventLoopGroup.shutdownGracefully();

    for (int i = 0; i < NUM_SERVERS; i++) {
      nettyServers[i].shutdownGracefully();
    }
  }

  @Test
  public void testTimeout() throws Exception {
    NettyServer[] nettyServers = new NettyServer[NUM_SERVERS];
    ServerInstance[] serverInstances = new ServerInstance[NUM_SERVERS];
    Map<ServerInstance, SegmentIdSet> serverToSegmentsMap = new HashMap<>(NUM_SERVERS);
    Map<ServerInstance, String> serverToRequestMap = new HashMap<>(NUM_SERVERS);

    for (int i = 0; i < NUM_SERVERS; i++) {
      int serverPort = BASE_SERVER_PORT + i;

      // Set first server as timeout server
      if (i == 0) {
        nettyServers[i] = new NettyTCPServer(serverPort, new TestRequestHandlerFactory(10_000L, false), null);
      } else {
        nettyServers[i] = new NettyTCPServer(serverPort, new TestRequestHandlerFactory(0L, false), null);
      }
      new Thread(nettyServers[i]).start();

      ServerInstance serverInstance = new ServerInstance(LOCAL_HOST, serverPort);
      serverInstances[i] = serverInstance;
      serverToSegmentsMap.put(serverInstance, new SegmentIdSet());
      serverToRequestMap.put(serverInstance, "request_" + i);
    }

    // Setup client
    MetricsRegistry metricsRegistry = new MetricsRegistry();
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    KeyedPoolImpl<PooledNettyClientResourceManager.PooledClientConnection> connectionPool =
        setUpConnectionPool(metricsRegistry, eventLoopGroup);
    ExecutorService executorService = Executors.newCachedThreadPool();
    ScatterGather scatterGather = new ScatterGatherImpl(connectionPool, executorService);
    ScatterGatherStats scatterGatherStats = new ScatterGatherStats();
    BrokerMetrics brokerMetrics = new BrokerMetrics(metricsRegistry);

    // Send the request
    ScatterGatherRequest scatterGatherRequest =
        new TestScatterGatherRequest(serverToSegmentsMap, serverToRequestMap, 1000L);
    CompositeFuture<ByteBuf> future =
        scatterGather.scatterGather(scatterGatherRequest, scatterGatherStats, brokerMetrics);

    // Should have no response from the error server
    Map<ServerInstance, ByteBuf> serverToResponseMap = future.get();
    Assert.assertEquals(serverToResponseMap.size(), NUM_SERVERS - 1);
    for (int i = 1; i < NUM_SERVERS; i++) {
      Assert.assertEquals(getResponse(serverToResponseMap.get(serverInstances[i])),
          serverToRequestMap.get(serverInstances[i]));
    }

    // Should get error from the timeout server
    Map<ServerInstance, Throwable> serverToErrorMap = future.getError();
    Assert.assertEquals(serverToErrorMap.size(), 1);
    Assert.assertTrue(serverToErrorMap.containsKey(serverInstances[0]));

    connectionPool.shutdown();
    executorService.shutdown();
    eventLoopGroup.shutdownGracefully();

    for (int i = 0; i < NUM_SERVERS; i++) {
      nettyServers[i].shutdownGracefully();
    }
  }

  @Test
  public void testError() throws Exception {
    NettyServer[] nettyServers = new NettyServer[NUM_SERVERS];
    ServerInstance[] serverInstances = new ServerInstance[NUM_SERVERS];
    Map<ServerInstance, SegmentIdSet> serverToSegmentsMap = new HashMap<>(NUM_SERVERS);
    Map<ServerInstance, String> serverToRequestMap = new HashMap<>(NUM_SERVERS);

    for (int i = 0; i < NUM_SERVERS; i++) {
      int serverPort = BASE_SERVER_PORT + i;

      // Set first server as error server
      if (i == 0) {
        // Must add a delay to prevent other requests getting shortcut
        nettyServers[i] = new NettyTCPServer(serverPort, new TestRequestHandlerFactory(1000L, true), null);
      } else {
        nettyServers[i] = new NettyTCPServer(serverPort, new TestRequestHandlerFactory(0L, false), null);
      }
      new Thread(nettyServers[i]).start();

      ServerInstance serverInstance = new ServerInstance(LOCAL_HOST, serverPort);
      serverInstances[i] = serverInstance;
      serverToSegmentsMap.put(serverInstance, new SegmentIdSet());
      serverToRequestMap.put(serverInstance, "request_" + i);
    }

    // Setup client
    MetricsRegistry metricsRegistry = new MetricsRegistry();
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    KeyedPoolImpl<PooledNettyClientResourceManager.PooledClientConnection> connectionPool =
        setUpConnectionPool(metricsRegistry, eventLoopGroup);
    ExecutorService executorService = Executors.newCachedThreadPool();
    ScatterGather scatterGather = new ScatterGatherImpl(connectionPool, executorService);
    ScatterGatherStats scatterGatherStats = new ScatterGatherStats();
    BrokerMetrics brokerMetrics = new BrokerMetrics(metricsRegistry);

    // Send the request
    ScatterGatherRequest scatterGatherRequest =
        new TestScatterGatherRequest(serverToSegmentsMap, serverToRequestMap, 10_000L);
    CompositeFuture<ByteBuf> future =
        scatterGather.scatterGather(scatterGatherRequest, scatterGatherStats, brokerMetrics);

    // Should have no response from the error server
    Map<ServerInstance, ByteBuf> serverToResponseMap = future.get();
    Assert.assertEquals(serverToResponseMap.size(), NUM_SERVERS - 1);
    for (int i = 1; i < NUM_SERVERS; i++) {
      Assert.assertEquals(getResponse(serverToResponseMap.get(serverInstances[i])),
          serverToRequestMap.get(serverInstances[i]));
    }

    // Should get error from the error server
    Map<ServerInstance, Throwable> serverToErrorMap = future.getError();
    Assert.assertEquals(serverToErrorMap.size(), 1);
    Assert.assertTrue(serverToErrorMap.containsKey(serverInstances[0]));

    connectionPool.shutdown();
    executorService.shutdown();
    eventLoopGroup.shutdownGracefully();

    for (int i = 0; i < NUM_SERVERS; i++) {
      nettyServers[i].shutdownGracefully();
    }
  }

  private KeyedPoolImpl<PooledNettyClientResourceManager.PooledClientConnection> setUpConnectionPool(
      MetricsRegistry metricsRegistry, EventLoopGroup eventLoopGroup) {
    ScheduledExecutorService timedExecutor = new ScheduledThreadPoolExecutor(1);
    ExecutorService poolExecutor = MoreExecutors.newDirectExecutorService();
    NettyClientMetrics clientMetrics = new NettyClientMetrics(metricsRegistry, "client_");
    PooledNettyClientResourceManager resourceManager =
        new PooledNettyClientResourceManager(eventLoopGroup, new HashedWheelTimer(), clientMetrics);
    KeyedPoolImpl<PooledNettyClientResourceManager.PooledClientConnection> connectionPool =
        new KeyedPoolImpl<>(1, 1, 300000, 1, resourceManager, timedExecutor, poolExecutor, metricsRegistry);
    resourceManager.setPool(connectionPool);
    return connectionPool;
  }

  private String getResponse(ByteBuf byteBuf) {
    byte[] bytes = new byte[byteBuf.readableBytes()];
    byteBuf.readBytes(bytes);
    return new String(bytes);
  }

  private static class TestScatterGatherRequest implements ScatterGatherRequest {
    private final Map<ServerInstance, SegmentIdSet> _serverToSegmentsMap;
    private final Map<ServerInstance, String> _serverToRequestMap;
    private final long _timeoutMs;

    public TestScatterGatherRequest(Map<ServerInstance, SegmentIdSet> serverToSegmentsMap,
        Map<ServerInstance, String> serverToRequestMap, long timeoutMs) {
      _serverToSegmentsMap = serverToSegmentsMap;
      _serverToRequestMap = serverToRequestMap;
      _timeoutMs = timeoutMs;
    }

    @Override
    public Map<ServerInstance, SegmentIdSet> getSegmentsServicesMap() {
      return _serverToSegmentsMap;
    }

    @Override
    public byte[] getRequestForService(ServerInstance service, SegmentIdSet queryPartitions) {
      return _serverToRequestMap.get(service).getBytes();
    }

    @Override
    public long getRequestId() {
      return 1L;
    }

    @Override
    public long getRequestTimeoutMs() {
      return _timeoutMs;
    }

    @Override
    public BrokerRequest getBrokerRequest() {
      return null;
    }
  }

  private static class TestRequestHandlerFactory implements RequestHandlerFactory {
    private final long _delayMs;
    private final boolean _throwError;

    public TestRequestHandlerFactory(long delayMs, boolean throwError) {
      _delayMs = delayMs;
      _throwError = throwError;
    }

    @Override
    public RequestHandler createNewRequestHandler() {
      return new RequestHandler() {
        @Override
        public ListenableFuture<byte[]> processRequest(ChannelHandlerContext channelHandlerContext, ByteBuf request) {
          Uninterruptibles.sleepUninterruptibly(_delayMs, TimeUnit.MILLISECONDS);

          if (_throwError) {
            throw new RuntimeException();
          }

          // Return the request as response
          byte[] requestBytes = new byte[request.readableBytes()];
          request.readBytes(requestBytes);
          return Futures.immediateFuture(requestBytes);
        }
      };
    }
  }
}

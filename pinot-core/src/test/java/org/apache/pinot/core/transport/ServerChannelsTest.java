/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.transport;

import com.sun.net.httpserver.HttpServer;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.util.concurrent.GenericFutureListener;
import java.net.InetSocketAddress;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class ServerChannelsTest {

  @DataProvider
  public Object[][] parameters() {
    return new Object[][]{
        new Object[]{true}, new Object[]{false},
    };
  }

  @BeforeClass
  public void setUp() {
    PinotMetricUtils.init(new PinotConfiguration());
    PinotMetricsRegistry registry = PinotMetricUtils.getPinotMetricsRegistry();
    BrokerMetrics.register(new BrokerMetrics(registry));
  }

  @Test(dataProvider = "parameters")
  public void testConnect(boolean nativeTransportEnabled)
      throws Exception {
    HttpServer dummyServer = HttpServer.create();
    dummyServer.bind(new InetSocketAddress("localhost", 0), 0);
    dummyServer.start();
    try {
      NettyConfig nettyConfig = new NettyConfig();
      nettyConfig.setNativeTransportsEnabled(nativeTransportEnabled);
      QueryRouter queryRouter = mock(QueryRouter.class);

      ServerRoutingInstance serverRoutingInstance =
          new ServerRoutingInstance("localhost", dummyServer.getAddress().getPort(), TableType.REALTIME);
      ServerChannels serverChannels =
          new ServerChannels(queryRouter, nettyConfig, null, ThreadAccountantUtils.getNoOpAccountant());
      serverChannels.connect(serverRoutingInstance);

      final long requestId = System.currentTimeMillis();

      AsyncQueryResponse asyncQueryResponse = mock(AsyncQueryResponse.class);
      BrokerRequest brokerRequest = new BrokerRequest();
      InstanceRequest instanceRequest = new InstanceRequest();
      instanceRequest.setRequestId(requestId);
      instanceRequest.setQuery(brokerRequest);
      serverChannels.sendRequest("dummy_table_name", asyncQueryResponse, serverRoutingInstance, instanceRequest, 1000);
      serverChannels.shutDown();
    } finally {
      dummyServer.stop(0);
    }
  }

  @Test
  public void testChannelsShareBufferAllocator() {
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), null, null, ThreadAccountantUtils.getNoOpAccountant());
    ServerChannels otherServerChannels =
        new ServerChannels(mock(QueryRouter.class), null, null, ThreadAccountantUtils.getNoOpAccountant());
    try {
      ByteBufAllocator allocator = getBootstrapAllocator(
          serverChannels.getOrCreateServerChannel(new ServerRoutingInstance("localhost", 12345, TableType.OFFLINE)));
      assertNotNull(allocator);
      assertSame(allocator, PooledByteBufAllocatorWithLimits.getSharedBufferAllocatorWithLimits());
      // All channels created by a ServerChannels use the same allocator
      assertSame(getBootstrapAllocator(serverChannels.getOrCreateServerChannel(
          new ServerRoutingInstance("localhost", 12346, TableType.REALTIME))), allocator);
      // Channels created by another ServerChannels instance (e.g. the TLS one) share it as well
      assertSame(getBootstrapAllocator(otherServerChannels.getOrCreateServerChannel(
          new ServerRoutingInstance("localhost", 12347, TableType.OFFLINE))), allocator);
    } finally {
      serverChannels.shutDown();
      otherServerChannels.shutDown();
    }
  }

  private static ByteBufAllocator getBootstrapAllocator(ServerChannels.ServerChannel serverChannel) {
    return (ByteBufAllocator) serverChannel._bootstrap.config().options().get(ChannelOption.ALLOCATOR);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWriteFailureClosesChannelAndFailsQuery() {
    QueryRouter queryRouter = mock(QueryRouter.class);
    ServerChannels serverChannels =
        new ServerChannels(queryRouter, null, null, ThreadAccountantUtils.getNoOpAccountant());

    ServerRoutingInstance routingInstance = new ServerRoutingInstance("localhost", 12345, TableType.OFFLINE);
    ServerChannels.ServerChannel serverChannel = serverChannels.getOrCreateServerChannel(routingInstance);

    Channel mockChannel = mock(Channel.class);
    ChannelFuture mockFuture = mock(ChannelFuture.class);
    when(mockChannel.writeAndFlush(any())).thenReturn(mockFuture);
    serverChannel.setChannel(mockChannel);

    ArgumentCaptor<GenericFutureListener> listenerCaptor = ArgumentCaptor.forClass(GenericFutureListener.class);
    when(mockFuture.addListener(listenerCaptor.capture())).thenReturn(mockFuture);

    AsyncQueryResponse asyncQueryResponse = mock(AsyncQueryResponse.class);
    serverChannel.sendRequestWithoutLocking("test_table", asyncQueryResponse, routingInstance, new byte[]{1, 2, 3});

    // Simulate write failure
    when(mockFuture.isSuccess()).thenReturn(false);
    when(mockFuture.cause()).thenReturn(new OutOfMemoryError("Direct buffer memory"));

    try {
      listenerCaptor.getValue().operationComplete(mockFuture);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    verify(mockChannel).close();

    verify(asyncQueryResponse).markServerUnavailable(any(ServerRoutingInstance.class), any(Exception.class));
    verify(asyncQueryResponse, never()).markRequestSent(any(ServerRoutingInstance.class), any(Integer.class));

    serverChannels.shutDown();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWriteSuccessMarksRequestSent() {
    QueryRouter queryRouter = mock(QueryRouter.class);
    ServerChannels serverChannels =
        new ServerChannels(queryRouter, null, null, ThreadAccountantUtils.getNoOpAccountant());

    ServerRoutingInstance routingInstance = new ServerRoutingInstance("localhost", 12345, TableType.OFFLINE);
    ServerChannels.ServerChannel serverChannel = serverChannels.getOrCreateServerChannel(routingInstance);

    Channel mockChannel = mock(Channel.class);
    ChannelFuture mockFuture = mock(ChannelFuture.class);
    when(mockChannel.writeAndFlush(any())).thenReturn(mockFuture);
    serverChannel.setChannel(mockChannel);

    ArgumentCaptor<GenericFutureListener> listenerCaptor = ArgumentCaptor.forClass(GenericFutureListener.class);
    when(mockFuture.addListener(listenerCaptor.capture())).thenReturn(mockFuture);

    AsyncQueryResponse asyncQueryResponse = mock(AsyncQueryResponse.class);
    serverChannel.sendRequestWithoutLocking("test_table", asyncQueryResponse, routingInstance, new byte[]{1, 2, 3});

    // Simulate write success
    when(mockFuture.isSuccess()).thenReturn(true);

    try {
      listenerCaptor.getValue().operationComplete(mockFuture);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    verify(asyncQueryResponse).markRequestSent(any(ServerRoutingInstance.class), any(Integer.class));
    verify(asyncQueryResponse, never()).markServerUnavailable(any(ServerRoutingInstance.class), any(Exception.class));
    verify(mockChannel, never()).close();

    serverChannels.shutDown();
  }

  /// The load-bearing guarantee for clusters that do not use the single-stage engine: pre-connect must
  /// not make `hasChannel()` true. `retryUnhealthyServer` reads it to decide whether the single-stage
  /// transport has any opinion on a server's health, and answers `UNKNOWN` when it does not. If opening a
  /// channel ahead of traffic flipped it, an MSE-only cluster would start voting servers `UNHEALTHY` on
  /// Netty reachability -- and that vote short-circuits the retrier loop before gRPC is ever consulted.
  @Test
  public void testSuccessfulPreConnectLeavesHasChannelFalse()
      throws Exception {
    HttpServer dummyServer = HttpServer.create();
    dummyServer.bind(new InetSocketAddress("localhost", 0), 0);
    dummyServer.start();
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), null, null, ThreadAccountantUtils.getNoOpAccountant());
    try {
      ServerRoutingInstance instance =
          new ServerRoutingInstance("localhost", dummyServer.getAddress().getPort(), TableType.OFFLINE);
      serverChannels.preConnect(instance, 5_000L);
      assertFalse(serverChannels.hasChannel(instance),
          "Pre-connect must not make the server look query-carrying to the failure detector");
    } finally {
      serverChannels.shutDown();
      dummyServer.stop(0);
    }
  }

  /// ... and a failed pre-connect must not either, so an unreachable server is not voted on.
  @Test
  public void testFailedPreConnectLeavesHasChannelFalse() {
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), null, null, ThreadAccountantUtils.getNoOpAccountant());
    // Port 1 on localhost: nothing listens, so the connect is refused rather than timing out.
    ServerRoutingInstance unreachable = new ServerRoutingInstance("localhost", 1, TableType.OFFLINE);
    try {
      assertThrows(Exception.class, () -> serverChannels.preConnect(unreachable, 5_000L));
      assertFalse(serverChannels.hasChannel(unreachable));
    } finally {
      serverChannels.shutDown();
    }
  }

  /// Sending a query is what makes a server query-carrying -- unchanged from before pre-connect existed,
  /// including when the connect itself fails.
  @Test
  public void testSendRequestMakesHasChannelTrue() {
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), null, null, ThreadAccountantUtils.getNoOpAccountant());
    ServerRoutingInstance unreachable = new ServerRoutingInstance("localhost", 1, TableType.OFFLINE);
    try {
      InstanceRequest instanceRequest = new InstanceRequest();
      instanceRequest.setRequestId(1L);
      instanceRequest.setQuery(new BrokerRequest());
      assertFalse(serverChannels.hasChannel(unreachable));
      assertThrows(Exception.class, () -> serverChannels.sendRequest("t", mock(AsyncQueryResponse.class), unreachable,
          instanceRequest, 5_000L));
      assertTrue(serverChannels.hasChannel(unreachable));
    } finally {
      serverChannels.shutDown();
    }
  }

  /// The point of pre-connect: the first query reuses the warm channel rather than opening a second one,
  /// and only then does the server become query-carrying.
  @Test
  public void testFirstQueryReusesThePreConnectedChannel()
      throws Exception {
    HttpServer dummyServer = HttpServer.create();
    dummyServer.bind(new InetSocketAddress("localhost", 0), 0);
    dummyServer.start();
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), null, null, ThreadAccountantUtils.getNoOpAccountant());
    try {
      ServerRoutingInstance instance =
          new ServerRoutingInstance("localhost", dummyServer.getAddress().getPort(), TableType.OFFLINE);
      serverChannels.preConnect(instance, 5_000L);
      ServerChannels.ServerChannel preConnected = serverChannels.getOrCreateServerChannel(instance);
      Channel warmChannel = preConnected._channel;
      assertNotNull(warmChannel);

      InstanceRequest instanceRequest = new InstanceRequest();
      instanceRequest.setRequestId(1L);
      instanceRequest.setQuery(new BrokerRequest());
      serverChannels.sendRequest("t", mock(AsyncQueryResponse.class), instance, instanceRequest, 5_000L);

      assertSame(serverChannels.getOrCreateServerChannel(instance), preConnected);
      assertSame(preConnected._channel, warmChannel, "The query must reuse the pre-connected channel");
      assertTrue(serverChannels.hasChannel(instance));
    } finally {
      serverChannels.shutDown();
      dummyServer.stop(0);
    }
  }
}

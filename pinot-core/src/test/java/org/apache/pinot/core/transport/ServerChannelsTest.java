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
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.GenericFutureListener;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
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
import static org.testng.Assert.assertEquals;
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
      ServerChannels.ServerChannel serverChannel = serverChannels.getOrCreateServerChannel(serverRoutingInstance);
      Channel connectedChannel = serverChannel._channel;
      org.testng.Assert.assertNotNull(connectedChannel);
      org.testng.Assert.assertTrue(connectedChannel.isOpen());

      final long requestId = System.currentTimeMillis();

      AsyncQueryResponse asyncQueryResponse = mock(AsyncQueryResponse.class);
      BrokerRequest brokerRequest = new BrokerRequest();
      InstanceRequest instanceRequest = new InstanceRequest();
      instanceRequest.setRequestId(requestId);
      instanceRequest.setQuery(brokerRequest);
      serverChannels.sendRequest("dummy_table_name", asyncQueryResponse, serverRoutingInstance, instanceRequest,
          1000);
      serverChannels.shutDown();
      org.testng.Assert.assertFalse(connectedChannel.isOpen());
    } finally {
      dummyServer.stop(0);
    }
  }

  @Test
  public void testClientTlsHandlerUsesPeerAddressForHostnameVerification() {
    TlsConfig tlsConfig = new TlsConfig();
    tlsConfig.setEndpointIdentificationAlgorithm("HTTPS");
    tlsConfig.setInsecure(true);

    SocketChannel channel = mock(SocketChannel.class);
    when(channel.alloc()).thenReturn(ByteBufAllocator.DEFAULT);

    SslHandler sslHandler =
        (SslHandler) ChannelHandlerFactory.getClientTlsHandler(tlsConfig, channel, "example.com", 8443);
    assertEquals(sslHandler.engine().getPeerHost(), "example.com");
    assertEquals(sslHandler.engine().getPeerPort(), 8443);
    assertEquals(sslHandler.engine().getSSLParameters().getEndpointIdentificationAlgorithm(), "HTTPS");
  }

  @Test
  public void testClientTlsHandlerClearsEndpointIdentificationAlgorithmWhenDisabled() {
    TlsConfig tlsConfig = new TlsConfig();
    tlsConfig.setEndpointIdentificationAlgorithm(" ");
    tlsConfig.setInsecure(true);

    SocketChannel channel = mock(SocketChannel.class);
    when(channel.alloc()).thenReturn(ByteBufAllocator.DEFAULT);

    SslHandler sslHandler =
        (SslHandler) ChannelHandlerFactory.getClientTlsHandler(tlsConfig, channel, "example.com", 8443);
    assertEquals(sslHandler.engine().getPeerHost(), "example.com");
    assertEquals(sslHandler.engine().getPeerPort(), 8443);
    String endpointIdentificationAlgorithm =
        sslHandler.engine().getSSLParameters().getEndpointIdentificationAlgorithm();
    assertTrue(endpointIdentificationAlgorithm == null || endpointIdentificationAlgorithm.isBlank());
  }

  @Test
  public void testQueryRouterShutDownTerminatesPlainAndTlsEventLoops()
      throws Exception {
    QueryRouter queryRouter = new QueryRouter("broker", null, new TlsConfig(), mock(ServerRoutingStatsManager.class),
        ThreadAccountantUtils.getNoOpAccountant());
    try {
      queryRouter.shutDown();
      assertTrue(getEventLoopGroup(queryRouter, "_serverChannels").isTerminated());
      assertTrue(getEventLoopGroup(queryRouter, "_serverChannelsTls").isTerminated());
    } finally {
      queryRouter.shutDown();
    }
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
    EventLoop mockEventLoop = mock(EventLoop.class);
    ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
    when(mockChannel.writeAndFlush(any())).thenReturn(mockFuture);
    when(mockChannel.pipeline()).thenReturn(mockPipeline);
    when(mockChannel.eventLoop()).thenReturn(mockEventLoop);
    when(mockEventLoop.inEventLoop()).thenReturn(false);
    when(mockPipeline.get(DirectOOMHandler.class)).thenReturn(null);
    when(mockChannel.close()).thenReturn(mockFuture);
    when(mockFuture.syncUninterruptibly()).thenReturn(mockFuture);
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
    verify(asyncQueryResponse).markServerDown(any(ServerRoutingInstance.class), any(Exception.class));
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
    EventLoop mockEventLoop = mock(EventLoop.class);
    ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
    when(mockChannel.writeAndFlush(any())).thenReturn(mockFuture);
    when(mockChannel.pipeline()).thenReturn(mockPipeline);
    when(mockChannel.eventLoop()).thenReturn(mockEventLoop);
    when(mockEventLoop.inEventLoop()).thenReturn(false);
    when(mockPipeline.get(DirectOOMHandler.class)).thenReturn(null);
    when(mockChannel.close()).thenReturn(mockFuture);
    when(mockFuture.syncUninterruptibly()).thenReturn(mockFuture);
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
    verify(asyncQueryResponse, never()).markServerDown(any(ServerRoutingInstance.class), any(Exception.class));
    verify(mockChannel, never()).close();

    serverChannels.shutDown();
  }

  @Test
  public void testCloseChannelDoesNotBlockOnEventLoopThread() {
    QueryRouter queryRouter = mock(QueryRouter.class);
    ServerChannels serverChannels =
        new ServerChannels(queryRouter, null, null, ThreadAccountantUtils.getNoOpAccountant());

    ServerRoutingInstance routingInstance = new ServerRoutingInstance("localhost", 12345, TableType.OFFLINE);
    ServerChannels.ServerChannel serverChannel = serverChannels.getOrCreateServerChannel(routingInstance);

    Channel mockChannel = mock(Channel.class);
    ChannelFuture mockFuture = mock(ChannelFuture.class);
    EventLoop mockEventLoop = mock(EventLoop.class);
    ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
    when(mockChannel.close()).thenReturn(mockFuture);
    when(mockChannel.eventLoop()).thenReturn(mockEventLoop);
    when(mockChannel.pipeline()).thenReturn(mockPipeline);
    when(mockEventLoop.inEventLoop()).thenReturn(true);
    when(mockPipeline.get(DirectOOMHandler.class)).thenReturn(null);
    serverChannel.setChannel(mockChannel);

    serverChannel.closeChannel();

    verify(mockChannel).close();
    verify(mockFuture).addListener(any());
    verify(mockFuture, never()).syncUninterruptibly();

    serverChannel.setChannel(null);
    serverChannels.shutDown();
  }

  private static EventLoopGroup getEventLoopGroup(QueryRouter queryRouter, String fieldName)
      throws ReflectiveOperationException {
    Field serverChannelsField = QueryRouter.class.getDeclaredField(fieldName);
    serverChannelsField.setAccessible(true);
    ServerChannels serverChannels = (ServerChannels) serverChannelsField.get(queryRouter);

    Field eventLoopGroupField = ServerChannels.class.getDeclaredField("_eventLoopGroup");
    eventLoopGroupField.setAccessible(true);
    return (EventLoopGroup) eventLoopGroupField.get(serverChannels);
  }
}

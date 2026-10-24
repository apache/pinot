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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.core.util.OsCheck;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.util.TestUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


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

  /// Connecting is bounded by the caller's timeout. Without that bound a connect to a host that drops packets -- the
  /// host the failure detector pings -- would block for Netty's 30 s default. 192.0.2.1 is reserved for
  /// documentation (RFC 5737) and routes nowhere: depending on the network the connect either hangs, which the timeout
  /// must cut short, or fails at once.
  @Test
  public void testConnectIsBoundedByTheCallerTimeout()
      throws Exception {
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), null, null, ThreadAccountantUtils.getNoOpAccountant());
    try {
      InstanceRequest instanceRequest = new InstanceRequest();
      instanceRequest.setRequestId(1L);
      instanceRequest.setQuery(new BrokerRequest());
      long timeoutMs = 500L;
      long startTimeMs = System.currentTimeMillis();
      try {
        serverChannels.sendRequest("dummy_table_name", mock(AsyncQueryResponse.class),
            new ServerRoutingInstance("192.0.2.1", 8098, TableType.OFFLINE), instanceRequest, timeoutMs);
        fail("Connecting to an unroutable address must fail");
      } catch (Exception e) {
        // A network that rejects the address fails the connect at once. One that drops the packets must hit the bound.
        if (System.currentTimeMillis() - startTimeMs >= timeoutMs) {
          assertTrue(e instanceof TimeoutException, "A hanging connect must end in the caller's timeout, got: " + e);
        }
      }
      assertTrue(System.currentTimeMillis() - startTimeMs < 5_000L, "The connect must be bounded by the timeout");
    } finally {
      serverChannels.shutDown();
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
    when(mockChannel.isWritable()).thenReturn(true);
    when(mockChannel.writeAndFlush(any())).thenReturn(mockFuture);
    serverChannel.setChannel(mockChannel);

    ArgumentCaptor<GenericFutureListener> listenerCaptor = ArgumentCaptor.forClass(GenericFutureListener.class);
    when(mockFuture.addListener(listenerCaptor.capture())).thenReturn(mockFuture);

    AsyncQueryResponse asyncQueryResponse = mock(AsyncQueryResponse.class);
    serverChannel.sendRequestWithoutLocking("test_table", asyncQueryResponse, routingInstance, new byte[]{1, 2, 3});

    // Simulate write failure
    when(mockFuture.isSuccess()).thenReturn(false);
    when(mockFuture.cause()).thenReturn(new OutOfMemoryError("Direct buffer memory"));
    // By the time the listener runs, a reconnect may have replaced the channel. Only the connection written to is
    // closed.
    Channel replacementChannel = mock(Channel.class);
    serverChannel.setChannel(replacementChannel);

    try {
      listenerCaptor.getValue().operationComplete(mockFuture);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    verify(mockChannel).close();
    verify(replacementChannel, never()).close();

    verify(asyncQueryResponse).markServerUnavailable(eq(routingInstance), eq(mockChannel), any(Exception.class),
        eq(true));
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
    when(mockChannel.isWritable()).thenReturn(true);
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
    verify(asyncQueryResponse, never()).markServerUnavailable(any(), any(), any(Exception.class), anyBoolean());
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

  /// Regression: the write-failure listener must close the channel it wrote to. It used to close whatever channel
  /// the entry held when the failure arrived, which after a reconnect is the healthy new channel.
  @SuppressWarnings("unchecked")
  @Test
  public void testLateWriteFailureClosesTheChannelItWroteTo() {
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), null, null, ThreadAccountantUtils.getNoOpAccountant());
    try {
      ServerRoutingInstance routingInstance = new ServerRoutingInstance("localhost", 12345, TableType.OFFLINE);
      ServerChannels.ServerChannel serverChannel = serverChannels.getOrCreateServerChannel(routingInstance);

      Channel staleChannel = mock(Channel.class);
      ChannelFuture mockFuture = mock(ChannelFuture.class);
      when(staleChannel.isWritable()).thenReturn(true);
      when(staleChannel.writeAndFlush(any())).thenReturn(mockFuture);
      ArgumentCaptor<GenericFutureListener> listenerCaptor = ArgumentCaptor.forClass(GenericFutureListener.class);
      when(mockFuture.addListener(listenerCaptor.capture())).thenReturn(mockFuture);
      serverChannel.setChannel(staleChannel);
      serverChannel.sendRequestWithoutLocking("test_table", mock(AsyncQueryResponse.class), routingInstance,
          new byte[]{1, 2, 3});

      // The entry reconnected before the write failure came back
      Channel freshChannel = mock(Channel.class);
      serverChannel.setChannel(freshChannel);
      when(mockFuture.isSuccess()).thenReturn(false);
      when(mockFuture.cause()).thenReturn(new IOException("Connection timed out"));
      try {
        listenerCaptor.getValue().operationComplete(mockFuture);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      verify(staleChannel).close();
      verify(freshChannel, never()).close();
    } finally {
      serverChannels.shutDown();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testEvictedLateWriteFailureDoesNotBlameServerOrAffectReplacement()
      throws Exception {
    ServerChannels channels =
        new ServerChannels(mock(QueryRouter.class), null, null, ThreadAccountantUtils.getNoOpAccountant());
    try {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", 12345, TableType.OFFLINE);
      ServerChannels.ServerChannel oldEntry = channels.getOrCreateServerChannel(instance);
      Channel oldChannel = mock(Channel.class);
      when(oldChannel.pipeline()).thenReturn(mock(ChannelPipeline.class));
      when(oldChannel.isWritable()).thenReturn(true);
      ChannelFuture oldWrite = mock(ChannelFuture.class);
      when(oldChannel.writeAndFlush(any())).thenReturn(oldWrite);
      ArgumentCaptor<GenericFutureListener> listener = ArgumentCaptor.forClass(GenericFutureListener.class);
      when(oldWrite.addListener(listener.capture())).thenReturn(oldWrite);
      oldEntry.setChannel(oldChannel);
      AsyncQueryResponse old = new AsyncQueryResponse(mock(QueryRouter.class), 1L, Set.of(instance),
          System.currentTimeMillis(), 10_000L, mock(ServerRoutingStatsManager.class), false);
      oldEntry.sendRequestWithoutLocking("testTable", old, instance, new byte[]{1});
      assertTrue(channels.evictChannel(instance));
      ServerChannels.ServerChannel replacement = channels.getOrCreateServerChannel(instance);
      assertNotSame(replacement, oldEntry);
      Channel freshChannel = mock(Channel.class);
      when(freshChannel.isWritable()).thenReturn(true);
      when(freshChannel.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));
      replacement.setChannel(freshChannel);
      AsyncQueryResponse fresh = new AsyncQueryResponse(mock(QueryRouter.class), 2L, Set.of(instance),
          System.currentTimeMillis(), 10_000L, mock(ServerRoutingStatsManager.class), false);
      replacement.sendRequestWithoutLocking("testTable", fresh, instance, new byte[]{2});
      when(oldWrite.isSuccess()).thenReturn(false);
      when(oldWrite.cause()).thenReturn(new IOException("evicted before write completed"));
      listener.getValue().operationComplete(oldWrite);
      assertEquals(old.getStatus(), QueryResponse.Status.FAILED);
      assertNotNull(old.getException());
      assertNull(old.getFailedServer(), "Broker eviction must not become a server failure via the write listener");
      assertEquals(fresh.getStatus(), QueryResponse.Status.IN_PROGRESS);
      assertNull(fresh.getException());
      assertNull(fresh.getFailedServer());
      verify(freshChannel, never()).close();
    } finally {
      channels.shutDown();
    }
  }

  /// A channel whose outbound queue is above the high watermark is not written to: the request fails right away
  /// instead of adding another buffer to the queue, and the channel is left to the write timeout handler.
  @Test
  public void testUnwritableChannelIsNotWrittenTo() {
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), null, null, ThreadAccountantUtils.getNoOpAccountant());
    try {
      ServerRoutingInstance routingInstance = new ServerRoutingInstance("localhost", 12345, TableType.OFFLINE);
      ServerChannels.ServerChannel serverChannel = serverChannels.getOrCreateServerChannel(routingInstance);
      Channel mockChannel = mock(Channel.class);
      when(mockChannel.isWritable()).thenReturn(false);
      when(mockChannel.bytesBeforeWritable()).thenReturn(12_345L);
      serverChannel.setChannel(mockChannel);
      AsyncQueryResponse asyncQueryResponse = mock(AsyncQueryResponse.class);

      long rejectionsBefore = meterCount(BrokerMeter.NETTY_CONNECTION_UNWRITABLE_REJECTIONS);
      IllegalStateException e = expectThrows(IllegalStateException.class,
          () -> serverChannel.sendRequestWithoutLocking("test_table", asyncQueryResponse, routingInstance,
              new byte[]{1, 2, 3}));
      assertTrue(e.getMessage().contains("not writable"), e.getMessage());
      assertTrue(e.getMessage().contains("12345"), e.getMessage());
      assertEquals(meterCount(BrokerMeter.NETTY_CONNECTION_UNWRITABLE_REJECTIONS), rejectionsBefore + 1);

      verify(mockChannel, never()).writeAndFlush(any());
      verify(mockChannel, never()).close();
      verify(asyncQueryResponse, never()).markRequestSent(any(), anyInt());
      verify(asyncQueryResponse, never()).markServerUnavailable(any(), any(), any(Exception.class), anyBoolean());
    } finally {
      serverChannels.shutDown();
    }
  }

  /// From the broker's side a peer that stopped draining its socket looks exactly like one that went away without a
  /// FIN: `isActive()` stays true and request writes stop completing. The write timeout is what notices, closes the
  /// channel, and thereby fails the query that was stuck in it.
  @Test
  public void testWriteTimeoutClosesStalledChannel()
      throws Exception {
    long writeTimeoutMs = 500L;
    NettyConfig nettyConfig = new NettyConfig();
    nettyConfig.setWriteTimeoutMs(writeTimeoutMs);
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), nettyConfig, null, ThreadAccountantUtils.getNoOpAccountant());
    try (StalledServer server = new StalledServer()) {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", server.getPort(), TableType.REALTIME);
      ServerChannels.ServerChannel serverChannel = serverChannels.getOrCreateServerChannel(instance);
      server.shrinkSendBuffer(serverChannel);
      AsyncQueryResponse asyncQueryResponse = mock(AsyncQueryResponse.class);
      long writeTimeoutsBefore = meterCount(BrokerMeter.NETTY_CONNECTION_WRITE_TIMEOUTS);

      long startTimeMs = System.currentTimeMillis();
      serverChannels.sendRequest("testTable", asyncQueryResponse, instance, largeInstanceRequest(4 * 1024 * 1024),
          10_000L);
      Channel channel = serverChannel._channel;
      assertNotNull(channel.pipeline().get(RequestWriteTimeoutHandler.class));
      // Not asserting isActive() here: on a slow host the timeout can fire before this line runs. The elapsed-time
      // check below is what proves the handler, not the kernel, closed the socket.

      TestUtils.waitForCondition(aVoid -> !channel.isActive(), 10L, 10_000L,
          "The write timeout did not close the stalled channel");
      long elapsedMs = System.currentTimeMillis() - startTimeMs;
      assertTrue(elapsedMs >= writeTimeoutMs, "Closed before the write timeout elapsed: " + elapsedMs + "ms");
      assertEquals(meterCount(BrokerMeter.NETTY_CONNECTION_WRITE_TIMEOUTS), writeTimeoutsBefore + 1);
      // Closing fails the stuck write, which marks the server unavailable for that query instead of letting it
      // wait out its own timeout
      verify(asyncQueryResponse, timeout(5_000L)).markServerUnavailable(eq(instance), eq(channel), any(Exception.class),
          eq(true));
      verify(asyncQueryResponse, never()).markRequestSent(eq(instance), anyInt());

      // The next query reconnects rather than reusing the closed channel
      AsyncQueryResponse nextResponse = mock(AsyncQueryResponse.class);
      serverChannels.sendRequest("testTable", nextResponse, instance, smallInstanceRequest(), 10_000L);
      assertNotSame(serverChannel._channel, channel);
      assertTrue(serverChannel._channel.isActive());
      verify(nextResponse, timeout(5_000L)).markRequestSent(eq(instance), anyInt());
    } finally {
      serverChannels.shutDown();
    }
  }

  /// The timeout is on progress, not on completion: a large request to a peer that drains slowly but steadily keeps
  /// making progress and must not be timed out however long it takes in all, since a false close costs every query in
  /// flight on the channel.
  ///
  /// The peer drains fast relative to the timeout but slow relative to the request: a receiver that reads slowly
  /// makes the kernel batch its window updates (silly window avoidance), so progress arrives in bursts spaced by
  /// about half a receive buffer worth of drain time, which must stay well inside the timeout for the test to
  /// measure the handler rather than the kernel. A slow network path has no such batching: acknowledgements free
  /// the send buffer continuously.
  @Test
  public void testSlowButProgressingWriteIsNotTimedOut()
      throws Exception {
    long writeTimeoutMs = 1_000L;
    NettyConfig nettyConfig = new NettyConfig();
    nettyConfig.setWriteTimeoutMs(writeTimeoutMs);
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), nettyConfig, null, ThreadAccountantUtils.getNoOpAccountant());
    // About 3MB/s: the 8MB request takes well over the timeout in all, with progress every few tens of milliseconds
    try (StalledServer server = new StalledServer(32 * 1024, 10L)) {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", server.getPort(), TableType.OFFLINE);
      ServerChannels.ServerChannel serverChannel = serverChannels.getOrCreateServerChannel(instance);
      server.shrinkSendBuffer(serverChannel);
      long writeTimeoutsBefore = meterCount(BrokerMeter.NETTY_CONNECTION_WRITE_TIMEOUTS);
      AsyncQueryResponse asyncQueryResponse = mock(AsyncQueryResponse.class);
      AsyncQueryResponse queuedResponse = mock(AsyncQueryResponse.class);

      long startTimeMs = System.currentTimeMillis();
      serverChannels.sendRequest("testTable", asyncQueryResponse, instance, largeInstanceRequest(8 * 1024 * 1024),
          10_000L);
      Channel channel = serverChannel._channel;
      // B is admitted behind A while the combined queue is below the high watermark. Only A gets socket progress.
      serverChannels.sendRequest("testTable", queuedResponse, instance, smallInstanceRequest(), 10_000L);
      Thread.sleep(writeTimeoutMs + 200L);
      assertTrue(server._bytesRead.get() > 0, "The peer must actually be draining A");
      verify(asyncQueryResponse, never()).markRequestSent(eq(instance), anyInt());
      verify(queuedResponse, never()).markRequestSent(eq(instance), anyInt());
      assertTrue(channel.isActive(), "A queued write must not time out while A keeps the socket progressing");
      verify(asyncQueryResponse, timeout(60_000L)).markRequestSent(eq(instance), anyInt());
      verify(queuedResponse, timeout(60_000L)).markRequestSent(eq(instance), anyInt());
      long elapsedMs = System.currentTimeMillis() - startTimeMs;
      assertTrue(elapsedMs > writeTimeoutMs, "The write must have outlived the timeout to prove the point: "
          + elapsedMs + "ms");
      assertTrue(channel.isActive(), "A progressing write must not get the channel closed");
      assertEquals(meterCount(BrokerMeter.NETTY_CONNECTION_WRITE_TIMEOUTS), writeTimeoutsBefore);
      verify(asyncQueryResponse, never()).markServerUnavailable(any(), any(), any(Exception.class), anyBoolean());
    } finally {
      serverChannels.shutDown();
    }
  }

  @DataProvider
  public Object[][] tlsDrain() {
    return new Object[][]{{true}, {false}};
  }

  /// Complete a real TLS handshake before timing application writes. Encryption must not hide socket progress.
  @Test(dataProvider = "tlsDrain")
  public void testTlsSocketProgressAndStall(boolean drain)
      throws Exception {
    long writeTimeoutMs = 1_000L;
    NettyConfig nettyConfig = new NettyConfig();
    nettyConfig.setWriteTimeoutMs(writeTimeoutMs);
    Path trustStore = Files.createTempFile("pinot-transport-trust", ".p12");
    try (InputStream input = getClass().getClassLoader().getResourceAsStream("tls/truststore.p12")) {
      assertNotNull(input);
      Files.copy(input, trustStore, StandardCopyOption.REPLACE_EXISTING);
    }
    TlsConfig tls = new TlsConfig();
    tls.setTrustStorePath(trustStore.toString());
    tls.setTrustStoreType("PKCS12");
    tls.setTrustStorePassword("changeit");
    ServerChannels channels =
        new ServerChannels(mock(QueryRouter.class), nettyConfig, tls, ThreadAccountantUtils.getNoOpAccountant());
    try (StalledServer server = new StalledServer(drain ? 32 * 1024 : 0, 10L, true)) {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", server.getPort(), TableType.OFFLINE);
      ServerChannels.ServerChannel entry = channels.getOrCreateServerChannel(instance);
      server.shrinkSendBuffer(entry);
      channels.preConnect(instance, 10_000L);
      Channel channel = entry._channel;
      AsyncQueryResponse response = new AsyncQueryResponse(mock(QueryRouter.class), 1L, Set.of(instance),
          System.currentTimeMillis(), 60_000L, mock(ServerRoutingStatsManager.class), false);
      long before = meterCount(BrokerMeter.NETTY_CONNECTION_WRITE_TIMEOUTS);
      long startMs = System.currentTimeMillis();
      channels.sendRequest("testTable", response, instance, largeInstanceRequest(8 * 1024 * 1024), 10_000L);
      if (drain) {
        TestUtils.waitForCondition(aVoid -> response.getCurrentResponses().get(instance).getRequestSentDelayMs() >= 0
            || !channel.isActive(), 10L, 60_000L, "TLS write did not finish");
        assertTrue(channel.isActive(), "Continuous ciphertext progress must not time out a slow TLS write");
        assertTrue(response.getCurrentResponses().get(instance).getRequestSentDelayMs() >= 0);
        assertTrue(System.currentTimeMillis() - startMs > writeTimeoutMs);
        assertTrue(server._bytesRead.get() > 0);
        assertNull(response.getException());
        assertEquals(meterCount(BrokerMeter.NETTY_CONNECTION_WRITE_TIMEOUTS), before);
      } else {
        TestUtils.waitForCondition(aVoid -> !channel.isActive(), 10L, 10_000L, "A stalled TLS write was not closed");
        TestUtils.waitForCondition(aVoid -> response.getException() != null, 10L, 5_000L,
            "The pending TLS request did not receive its write failure");
        assertEquals(meterCount(BrokerMeter.NETTY_CONNECTION_WRITE_TIMEOUTS), before + 1);
        assertEquals(response.getStatus(), QueryResponse.Status.FAILED);
      }
    } finally {
      channels.shutDown();
      Files.deleteIfExists(trustStore);
    }
  }

  /// Holding N writes back at the socket must do N first-flush operations, rather than revisit the entire backlog.
  @Test
  public void testFlushOnlyVisitsNewlyUnflushedWrites() {
    EmbeddedChannel channel = stalledEmbeddedChannel(60_000L);
    AtomicInteger firstFlushOperations = new AtomicInteger();
    int writes = 64;
    try {
      for (int i = 0; i < writes; i++) {
        channel.writeAndFlush(Unpooled.buffer(8).writeLong(i), countedPromise(channel, firstFlushOperations));
      }
      assertEquals(firstFlushOperations.get(), writes,
          "Each relay must participate only in its first flush, regardless of the held socket backlog");
      for (int i = 0; i < writes; i++) {
        channel.flush();
      }
      assertEquals(firstFlushOperations.get(), writes, "Empty flushes must not revisit pending writes");
      ByteBuf cancelledBytes = Unpooled.buffer(8).writeLong(writes);
      ChannelPromise cancelled = countedPromise(channel, firstFlushOperations);
      channel.write(cancelledBytes, cancelled);
      assertTrue(cancelled.cancel(false));
      channel.flush();
      assertEquals(firstFlushOperations.get(), writes, "A pre-flush cancellation must leave no first-flush work");
      assertEquals(cancelledBytes.refCnt(), 0);
      assertTrue(channel.isActive());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  private static ChannelPromise countedPromise(Channel channel, AtomicInteger firstFlushOperations) {
    return new DefaultChannelPromise(channel) {
      @Override
      public boolean setUncancellable() {
        firstFlushOperations.incrementAndGet();
        return super.setUncancellable();
      }
    };
  }

  @Test
  public void testCancelledQueuedWriteReleasesBuffer() {
    EmbeddedChannel channel = stalledEmbeddedChannel(100L);
    try {
      ByteBuf bytes = Unpooled.buffer(8).writeLong(1L);
      ChannelPromise promise = channel.newPromise();
      channel.write(bytes, promise);
      assertTrue(promise.cancel(false));
      channel.flush();
      assertEquals(bytes.refCnt(), 0, "Cancellation before flush must retain Netty's release/skip behavior");
      assertTrue(promise.isCancelled());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test(dataProvider = "tlsDrain")
  public void testFlushedRegularAndVoidWritesRemainMonitored(boolean voidPromise)
      throws Exception {
    EmbeddedChannel channel = stalledEmbeddedChannel(150L);
    long before = meterCount(BrokerMeter.NETTY_CONNECTION_WRITE_TIMEOUTS);
    try {
      ByteBuf bytes = Unpooled.buffer(8).writeLong(1L);
      ChannelPromise promise = voidPromise ? channel.voidPromise() : channel.newPromise();
      channel.writeAndFlush(bytes, promise);
      if (!voidPromise) {
        assertFalse(promise.cancel(false), "A flushed write must retain Netty's uncancelable promise contract");
      }
      Thread.sleep(200L);
      channel.runScheduledPendingTasks();
      assertFalse(channel.isActive(), "Already flushed bytes must still be monitored, including void promises");
      assertEquals(bytes.refCnt(), 0);
      if (!voidPromise) {
        assertTrue(promise.isDone());
        assertFalse(promise.isSuccess());
        assertNotNull(promise.cause());
      }
      assertEquals(meterCount(BrokerMeter.NETTY_CONNECTION_WRITE_TIMEOUTS), before + 1);
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  public void testCompletionFailureAndRemovalRelayRealOutcomes()
      throws Exception {
    EmbeddedChannel channel = stalledEmbeddedChannel(150L);
    try {
      ByteBuf successBytes = Unpooled.buffer(8).writeLong(1L);
      ChannelPromise success = channel.newPromise();
      channel.writeAndFlush(successBytes, success);
      channel.unsafe().outboundBuffer().progress(8L);
      channel.unsafe().outboundBuffer().remove();
      assertTrue(success.isSuccess());
      assertEquals(successBytes.refCnt(), 0);
      Thread.sleep(200L);
      channel.runScheduledPendingTasks();
      assertTrue(channel.isActive(), "The drained queue must cancel its timer");
      ByteBuf failedBytes = Unpooled.buffer(8).writeLong(2L);
      ChannelPromise failed = channel.newPromise();
      channel.writeAndFlush(failedBytes, failed);
      IOException failure = new IOException("socket write failed");
      channel.unsafe().outboundBuffer().remove(failure);
      assertSame(failed.cause(), failure);
      assertEquals(failedBytes.refCnt(), 0);
      ByteBuf removedBytes = Unpooled.buffer(8).writeLong(3L);
      ChannelPromise removed = channel.newPromise();
      channel.writeAndFlush(removedBytes, removed);
      channel.pipeline().remove(RequestWriteTimeoutHandler.class);
      Thread.sleep(200L);
      channel.runScheduledPendingTasks();
      assertTrue(channel.isActive(), "Removing the monitor must cancel the timer");
      assertFalse(removed.isDone(), "Handler removal must not invent downstream completion");
      channel.unsafe().outboundBuffer().remove(failure);
      assertSame(removed.cause(), failure);
      assertEquals(removedBytes.refCnt(), 0);
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  public void testLaterEnqueuesDoNotPostponeSocketIdleTimeout()
      throws Exception {
    EmbeddedChannel channel = stalledEmbeddedChannel(200L);
    List<ByteBuf> bytes = new ArrayList<>();
    List<ChannelPromise> promises = new ArrayList<>();
    long before = meterCount(BrokerMeter.NETTY_CONNECTION_WRITE_TIMEOUTS);
    try {
      for (int i = 0; i < 3; i++) {
        bytes.add(Unpooled.buffer(8).writeLong(i));
        promises.add(channel.newPromise());
        channel.writeAndFlush(bytes.get(i), promises.get(i));
        Thread.sleep(80L);
      }
      channel.runScheduledPendingTasks();
      assertFalse(channel.isActive(), "Enqueue is not physical socket progress");
      for (int i = 0; i < 3; i++) {
        assertEquals(bytes.get(i).refCnt(), 0);
        assertTrue(promises.get(i).isDone());
        assertFalse(promises.get(i).isSuccess());
      }
      assertEquals(meterCount(BrokerMeter.NETTY_CONNECTION_WRITE_TIMEOUTS), before + 1);
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  /// Use Netty's actual outbound queue and flush/cancellation semantics while deliberately accepting no bytes.
  private static EmbeddedChannel stalledEmbeddedChannel(long timeoutMs) {
    return new EmbeddedChannel(new RequestWriteTimeoutHandler(
        new ServerRoutingInstance("localhost", 12345, TableType.OFFLINE), timeoutMs),
        new ChannelInboundHandlerAdapter() {
          @Override
          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // Void promises retain Netty's exception propagation on close; only that expected terminal event is sunk.
            if (!(cause instanceof ClosedChannelException)) {
              ctx.fireExceptionCaught(cause);
            }
          }
        }) {
      @Override
      protected void doWrite(ChannelOutboundBuffer outboundBuffer) {
        // A stalled socket leaves the flushed queue intact until the real channel close fails/releases its entries.
      }
    };
  }

  /// The memory bound: once the outbound queue of a stalled channel passes the high watermark, requests to that
  /// server fail fast instead of each adding a request buffer to direct memory.
  @Test
  public void testStalledChannelRejectsRequestsAboveTheHighWaterMark()
      throws Exception {
    NettyConfig nettyConfig = new NettyConfig();
    // Keep the stalled channel open so that writability, not the write timeout, is what is exercised
    nettyConfig.setWriteTimeoutMs(0L);
    nettyConfig.setWriteBufferWaterMarks(32 * 1024, 64 * 1024);
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), nettyConfig, null, ThreadAccountantUtils.getNoOpAccountant());
    try (StalledServer server = new StalledServer()) {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", server.getPort(), TableType.OFFLINE);
      ServerChannels.ServerChannel serverChannel = serverChannels.getOrCreateServerChannel(instance);
      server.shrinkSendBuffer(serverChannel);
      long rejectionsBefore = meterCount(BrokerMeter.NETTY_CONNECTION_UNWRITABLE_REJECTIONS);

      // The first request is accepted: the queue was empty. It then sits in the queue because the peer does not drain.
      serverChannels.sendRequest("testTable", mock(AsyncQueryResponse.class), instance,
          largeInstanceRequest(1024 * 1024), 10_000L);
      Channel channel = serverChannel._channel;
      assertNull(channel.pipeline().get(RequestWriteTimeoutHandler.class), "Write timeout was disabled");
      // Netty counts a write submitted off the event loop against the watermark right away, uncounts it for an instant
      // when the event loop picks the task up, and counts it again once it sits in the outbound buffer. Wait for that
      // last state (flushed, not written) so the check below cannot land in the instant in between.
      TestUtils.waitForCondition(aVoid -> !channel.isWritable() && channel.unsafe().outboundBuffer() != null
          && channel.unsafe().outboundBuffer().size() > 0, 10L, 5_000L,
          "The channel did not become unwritable with 1MB queued against a 64KB high watermark");

      AsyncQueryResponse rejectedResponse = mock(AsyncQueryResponse.class);
      long startTimeMs = System.currentTimeMillis();
      IllegalStateException e = expectThrows(IllegalStateException.class,
          () -> serverChannels.sendRequest("testTable", rejectedResponse, instance, smallInstanceRequest(), 10_000L));
      assertTrue(System.currentTimeMillis() - startTimeMs < 2_000L, "The rejection must not wait");
      assertTrue(e.getMessage().contains("not writable"), e.getMessage());
      assertEquals(meterCount(BrokerMeter.NETTY_CONNECTION_UNWRITABLE_REJECTIONS), rejectionsBefore + 1);
      verify(rejectedResponse, never()).markRequestSent(any(), anyInt());
      // Rejecting does not close the channel; the write timeout handler owns that decision
      assertTrue(channel.isActive());
      assertSame(serverChannel._channel, channel);
    } finally {
      serverChannels.shutDown();
    }
  }

  /// Readmission path: evicting closes the cached channel and forgets it, so the next request opens a fresh one.
  @Test
  public void testEvictChannelClosesItAndNextRequestReconnects()
      throws Exception {
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), null, null, ThreadAccountantUtils.getNoOpAccountant());
    try (StalledServer server = new StalledServer()) {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", server.getPort(), TableType.REALTIME);
      // Nothing cached yet: nothing to evict
      assertFalse(serverChannels.evictChannel(instance));

      serverChannels.sendRequest("testTable", mock(AsyncQueryResponse.class), instance, smallInstanceRequest(),
          10_000L);
      ServerChannels.ServerChannel staleEntry = serverChannels.getOrCreateServerChannel(instance);
      Channel staleChannel = staleEntry._channel;
      assertTrue(staleChannel.isActive());
      assertTrue(serverChannels.hasChannel(instance));
      long evictionsBefore = meterCount(BrokerMeter.NETTY_CONNECTION_CHANNEL_EVICTIONS);

      assertTrue(serverChannels.evictChannel(instance));
      assertEquals(meterCount(BrokerMeter.NETTY_CONNECTION_CHANNEL_EVICTIONS), evictionsBefore + 1);
      TestUtils.waitForCondition(aVoid -> !staleChannel.isActive(), 10L, 5_000L, "Evicted channel was not closed");
      assertFalse(serverChannels.evictChannel(instance), "Already evicted");
      // The transport has no channel to vouch for any more
      assertFalse(serverChannels.hasChannel(instance));

      AsyncQueryResponse nextResponse = mock(AsyncQueryResponse.class);
      serverChannels.sendRequest("testTable", nextResponse, instance, smallInstanceRequest(), 10_000L);
      ServerChannels.ServerChannel freshEntry = serverChannels.getOrCreateServerChannel(instance);
      assertNotSame(freshEntry, staleEntry);
      assertNotSame(freshEntry._channel, staleChannel);
      assertTrue(freshEntry._channel.isActive());
      verify(nextResponse, timeout(5_000L)).markRequestSent(eq(instance), anyInt());
      assertTrue(serverChannels.hasChannel(instance));
    } finally {
      serverChannels.shutDown();
    }
  }

  /// The race eviction has to survive: a sender that fetched the entry before it was evicted must neither write into
  /// the evicted channel nor reconnect an entry that nobody can look up any more (which would leak a channel). The
  /// entry reports the eviction and the public send path retries on a fresh entry.
  @Test
  public void testSenderHoldingAnEvictedEntryRetriesOnAFreshOne()
      throws Exception {
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), null, null, ThreadAccountantUtils.getNoOpAccountant());
    try (StalledServer server = new StalledServer()) {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", server.getPort(), TableType.OFFLINE);
      ServerChannels.ServerChannel staleEntry = serverChannels.getOrCreateServerChannel(instance);
      serverChannels.connect(instance);
      Channel staleChannel = staleEntry._channel;
      assertTrue(staleChannel.isActive());

      assertTrue(serverChannels.evictChannel(instance));
      TestUtils.waitForCondition(aVoid -> !staleChannel.isActive(), 10L, 5_000L, "Evicted channel was not closed");

      // A sender still holding the evicted entry: refused, and the entry is not reconnected
      AsyncQueryResponse staleResponse = mock(AsyncQueryResponse.class);
      assertNull(staleEntry.sendRequest("testTable", staleResponse, instance, new byte[]{1, 2, 3}, 10_000L));
      assertSame(staleEntry._channel, staleChannel, "An evicted entry must not reconnect");
      verify(staleResponse, never()).markRequestSent(any(), anyInt());
      // Nor can the failure detector's probe or startup pre-connect resurrect it
      assertThrows(IllegalStateException.class, staleEntry::connect);
      assertThrows(IllegalStateException.class, () -> staleEntry.preConnect(5_000L));
      assertSame(staleEntry._channel, staleChannel);

      // The public path retries transparently on a fresh entry
      AsyncQueryResponse freshResponse = mock(AsyncQueryResponse.class);
      serverChannels.sendRequest("testTable", freshResponse, instance, smallInstanceRequest(), 10_000L);
      ServerChannels.ServerChannel freshEntry = serverChannels.getOrCreateServerChannel(instance);
      assertNotSame(freshEntry, staleEntry);
      assertTrue(freshEntry._channel.isActive());
      verify(freshResponse, timeout(5_000L)).markRequestSent(eq(instance), anyInt());
    } finally {
      serverChannels.shutDown();
    }
  }

  /// The kernel-level half-open detection is a Linux socket option: applied on the Epoll transport, skipped (not
  /// passed to Netty, which would only log a warning and ignore it) everywhere else. The watermark applies everywhere.
  @Test(dataProvider = "parameters")
  public void testLivenessOptionsOnBootstrap(boolean nativeTransportEnabled) {
    NettyConfig nettyConfig = new NettyConfig();
    nettyConfig.setNativeTransportsEnabled(nativeTransportEnabled);
    nettyConfig.setTcpUserTimeoutMs(1_234);
    nettyConfig.setWriteBufferWaterMarks(1_000, 2_000);
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), nettyConfig, null, ThreadAccountantUtils.getNoOpAccountant());
    try {
      Map<ChannelOption<?>, Object> options = serverChannels.getOrCreateServerChannel(
          new ServerRoutingInstance("localhost", 12345, TableType.OFFLINE))._bootstrap.config().options();
      WriteBufferWaterMark waterMark = (WriteBufferWaterMark) options.get(ChannelOption.WRITE_BUFFER_WATER_MARK);
      assertNotNull(waterMark);
      assertEquals(waterMark.low(), 1_000);
      assertEquals(waterMark.high(), 2_000);

      boolean epoll = nativeTransportEnabled && OsCheck.getOperatingSystemType() == OsCheck.OSType.Linux
          && Epoll.isAvailable();
      assertEquals(serverChannels.isTcpUserTimeoutApplied(), epoll);
      assertEquals(options.containsKey(EpollChannelOption.TCP_USER_TIMEOUT), epoll);
      if (epoll) {
        assertEquals(options.get(EpollChannelOption.TCP_USER_TIMEOUT), 1_234);
      }
    } finally {
      serverChannels.shutDown();
    }
  }

  /// Turning the TCP user timeout off keeps the option off the bootstrap even on Epoll.
  @Test
  public void testTcpUserTimeoutDisabled() {
    NettyConfig nettyConfig = new NettyConfig();
    nettyConfig.setNativeTransportsEnabled(true);
    nettyConfig.setTcpUserTimeoutMs(0);
    ServerChannels serverChannels =
        new ServerChannels(mock(QueryRouter.class), nettyConfig, null, ThreadAccountantUtils.getNoOpAccountant());
    try {
      assertFalse(serverChannels.isTcpUserTimeoutApplied());
      assertFalse(serverChannels.getOrCreateServerChannel(new ServerRoutingInstance("localhost", 12345,
          TableType.OFFLINE))._bootstrap.config().options().containsKey(EpollChannelOption.TCP_USER_TIMEOUT));
    } finally {
      serverChannels.shutDown();
    }
  }

  private static long meterCount(BrokerMeter meter) {
    return BrokerMetrics.get().getMeteredValue(meter).count();
  }

  private static InstanceRequest smallInstanceRequest() {
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(1L);
    instanceRequest.setQuery(new BrokerRequest());
    return instanceRequest;
  }

  /// A request of roughly the given serialized size, padded with segment names: what a query over a table with many
  /// segments per server looks like on the wire.
  private static InstanceRequest largeInstanceRequest(int approximateBytes) {
    InstanceRequest instanceRequest = smallInstanceRequest();
    String segmentName = "segment_" + "x".repeat(56);
    int numSegments = approximateBytes / segmentName.length();
    List<String> segments = new ArrayList<>(numSegments);
    for (int i = 0; i < numSegments; i++) {
      segments.add(segmentName);
    }
    instanceRequest.setSearchSegments(segments);
    return instanceRequest;
  }

  /// A server that accepts connections and, by default, never reads from them, with the smallest socket buffers the
  /// OS allows. Once those buffers are full the client's writes stop completing while its socket still reports
  /// active: the closest a loopback test gets to a peer that went away without a FIN, or a path that black-holes
  /// packets. Given a drain size and interval, it instead reads that much every interval: a slow but live peer.
  private static class StalledServer implements AutoCloseable {
    private static final int SOCKET_BUFFER_BYTES = 4096;

    private final ServerSocket _serverSocket;
    private final List<Socket> _accepted = new CopyOnWriteArrayList<>();
    private final Thread _acceptor;
    private final AtomicLong _bytesRead = new AtomicLong();

    StalledServer()
        throws Exception {
      this(0, 0L);
    }

    StalledServer(int drainBytes, long drainIntervalMs)
        throws Exception {
      this(drainBytes, drainIntervalMs, false);
    }

    StalledServer(int drainBytes, long drainIntervalMs, boolean tls)
        throws Exception {
      if (tls) {
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("tls/keystore.p12")) {
          keys.load(input, "changeit".toCharArray());
        }
        KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        factory.init(keys, "changeit".toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(factory.getKeyManagers(), null, null);
        _serverSocket = context.getServerSocketFactory().createServerSocket();
      } else {
        _serverSocket = new ServerSocket();
      }
      // Must be set before bind so that the accepted sockets inherit it
      _serverSocket.setReceiveBufferSize(SOCKET_BUFFER_BYTES);
      _serverSocket.bind(new InetSocketAddress("localhost", 0));
      _acceptor = new Thread(() -> {
        while (!_serverSocket.isClosed()) {
          try {
            Socket socket = _serverSocket.accept();
            _accepted.add(socket);
            if (socket instanceof SSLSocket) {
              ((SSLSocket) socket).startHandshake();
            }
            if (drainBytes > 0) {
              Thread drainer = new Thread(() -> drainSlowly(socket, drainBytes, drainIntervalMs),
                  "stalled-server-drainer");
              drainer.setDaemon(true);
              drainer.start();
            }
          } catch (IOException e) {
            return;
          }
        }
      }, "stalled-server-acceptor");
      _acceptor.setDaemon(true);
      _acceptor.start();
    }

    private void drainSlowly(Socket socket, int drainBytes, long drainIntervalMs) {
      byte[] buffer = new byte[drainBytes];
      try {
        while (!socket.isClosed()) {
          int remaining = drainBytes;
          while (remaining > 0) {
            int read = socket.getInputStream().read(buffer, 0, remaining);
            if (read < 0) {
              return;
            }
            remaining -= read;
            _bytesRead.addAndGet(read);
          }
          Thread.sleep(drainIntervalMs);
        }
      } catch (IOException | InterruptedException e) {
        // Closed, or the test is over
      }
    }

    int getPort() {
      return _serverSocket.getLocalPort();
    }

    /// Shrinks the client's send buffer too, so that a request of a few hundred KB is enough to stall the channel.
    void shrinkSendBuffer(ServerChannels.ServerChannel serverChannel) {
      serverChannel._bootstrap.option(ChannelOption.SO_SNDBUF, SOCKET_BUFFER_BYTES);
    }

    @Override
    public void close() {
      IOUtils.closeQuietly(_serverSocket);
      for (Socket socket : _accepted) {
        IOUtils.closeQuietly(socket);
      }
    }
  }
}

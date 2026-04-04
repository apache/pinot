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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class QueryServerTest {
  @DataProvider
  public Object[][] parameters() {
    return new Object[][]{
        new Object[]{true}, new Object[]{false},
    };
  }

  @Test(dataProvider = "parameters")
  public void startAndStop(final boolean nativeTransportEnabled) {
    PinotMetricUtils.init(new PinotConfiguration());
    PinotMetricsRegistry registry = PinotMetricUtils.getPinotMetricsRegistry();
    ServerMetrics.register(new ServerMetrics(registry));
    NettyConfig nettyConfig = new NettyConfig();
    nettyConfig.setNativeTransportsEnabled(nativeTransportEnabled);
    TlsConfig tlsConfig = new TlsConfig();
    ChannelHandler channelHandler = mock(ChannelHandler.class);

    QueryServer server = new QueryServer(0, nettyConfig, tlsConfig, channelHandler);
    server.start();

    final InetSocketAddress serverAddress = server.getChannel().localAddress();

    assertTrue(connectionOk(serverAddress));

    server.shutDown();
    assertFalse(connectionOk(serverAddress));
  }

  @Test
  public void testAllChannelsCleanupOnClose()
      throws Exception {
    PinotMetricUtils.init(new PinotConfiguration());
    PinotMetricsRegistry registry = PinotMetricUtils.getPinotMetricsRegistry();
    ServerMetrics.register(new ServerMetrics(registry));
    QueryServer server = new QueryServer(0, new NettyConfig(), null, mock(ChannelHandler.class));
    server.start();

    InetSocketAddress serverAddress = server.getChannel().localAddress();
    Socket socket = new Socket(serverAddress.getHostName(), serverAddress.getPort());

    try {
      TestUtils.waitForCondition(aVoid -> server.getConnectedChannelCount() > 0, 5_000L,
          "Channel was not registered in _allChannels");

      socket.close();

      TestUtils.waitForCondition(aVoid -> server.getConnectedChannelCount() == 0, 5_000L,
          "Channel was not removed from _allChannels after close");
    } finally {
      IOUtils.closeQuietly(socket);
      server.shutDown();
    }
  }

  @Test
  public void testServerCanRestartOnSamePortImmediately() {
    PinotMetricUtils.init(new PinotConfiguration());
    PinotMetricsRegistry registry = PinotMetricUtils.getPinotMetricsRegistry();
    ServerMetrics.register(new ServerMetrics(registry));

    QueryServer firstServer = new QueryServer(0, new NettyConfig(), null, mock(ChannelHandler.class));
    firstServer.start();
    int port = firstServer.getChannel().localAddress().getPort();
    firstServer.shutDown();

    QueryServer secondServer = new QueryServer(port, new NettyConfig(), null, mock(ChannelHandler.class));
    secondServer.start();
    try {
      assertTrue(connectionOk(secondServer.getChannel().localAddress()));
    } finally {
      secondServer.shutDown();
    }
  }

  @Test
  public void testCloseAcceptedChannelSnapshotClosesRemainingChannelsAfterTimeout() {
    QueryServer server = new QueryServer(0, new NettyConfig(), null, mock(ChannelHandler.class));
    try {
      SocketChannel slowChannel = mock(SocketChannel.class);
      SocketChannel fastChannel = mock(SocketChannel.class);
      ChannelFuture slowCloseFuture = mock(ChannelFuture.class);
      ChannelFuture fastCloseFuture = mock(ChannelFuture.class);

      org.mockito.InOrder inOrder = inOrder(slowChannel, fastChannel, slowCloseFuture, fastCloseFuture);
      when(slowChannel.close()).thenReturn(slowCloseFuture);
      when(fastChannel.close()).thenReturn(fastCloseFuture);
      when(slowChannel.closeFuture()).thenReturn(slowCloseFuture);
      when(fastChannel.closeFuture()).thenReturn(fastCloseFuture);
      when(slowCloseFuture.awaitUninterruptibly(anyLong())).thenReturn(false);
      when(fastCloseFuture.awaitUninterruptibly(anyLong())).thenReturn(true);

      server.closeAcceptedChannelSnapshot(new SocketChannel[]{slowChannel, fastChannel},
          System.currentTimeMillis() + 1_000L);

      inOrder.verify(slowChannel).close();
      inOrder.verify(fastChannel).close();
      inOrder.verify(slowChannel).closeFuture();
      inOrder.verify(slowCloseFuture).awaitUninterruptibly(anyLong());
      inOrder.verify(fastChannel).closeFuture();
      inOrder.verify(fastCloseFuture).awaitUninterruptibly(anyLong());
      verify(fastChannel).close();
    } finally {
      server.shutDown();
    }
  }

  private static boolean connectionOk(InetSocketAddress address) {
    Socket s = null;
    try {
      s = new Socket(address.getHostName(), address.getPort());
      return s.isConnected();
    } catch (Exception e) {
      return false;
    } finally {
      IOUtils.closeQuietly(s);
    }
  }
}

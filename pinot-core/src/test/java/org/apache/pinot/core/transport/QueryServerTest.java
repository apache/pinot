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

import io.netty.channel.ChannelHandler;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
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

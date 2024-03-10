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
import java.net.InetSocketAddress;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class ServerChannelsTest {
  private static HttpServer _dummyServer;

  @DataProvider
  public Object[][] parameters() {
    return new Object[][]{
        new Object[]{true}, new Object[]{false},
    };
  }

  @BeforeClass
  public void beforeClass()
      throws Exception {
    _dummyServer = HttpServer.create();
    _dummyServer.bind(new InetSocketAddress("localhost", 0), 0);
    _dummyServer.start();
  }

  @AfterClass
  public void afterClass()
      throws Exception {
    if (_dummyServer != null) {
      _dummyServer.stop(0);
    }
  }

  @Test(dataProvider = "parameters")
  public void testConnect(final boolean nativeTransportEnabled)
      throws Exception {
    PinotMetricUtils.init(new PinotConfiguration());
    PinotMetricsRegistry registry = PinotMetricUtils.getPinotMetricsRegistry();
    BrokerMetrics brokerMetrics = new BrokerMetrics(registry);
    NettyConfig nettyConfig = new NettyConfig();
    nettyConfig.setNativeTransportsEnabled(nativeTransportEnabled);
    QueryRouter queryRouter = mock(QueryRouter.class);

    ServerRoutingInstance serverRoutingInstance =
        new ServerRoutingInstance("localhost", _dummyServer.getAddress().getPort(), TableType.REALTIME);
    ServerChannels serverChannels = new ServerChannels(queryRouter, brokerMetrics, nettyConfig, null);
    serverChannels.connect(serverRoutingInstance);

    final long requestId = System.currentTimeMillis();

    AsyncQueryResponse asyncQueryResponse = mock(AsyncQueryResponse.class);
    BrokerRequest brokerRequest = new BrokerRequest();
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(requestId);
    instanceRequest.setQuery(brokerRequest);
    serverChannels.sendRequest("dummy_table_name", asyncQueryResponse, serverRoutingInstance, instanceRequest, 1000);
    serverChannels.shutDown();
  }
}

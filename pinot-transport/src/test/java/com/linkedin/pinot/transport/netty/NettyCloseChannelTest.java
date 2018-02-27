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

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.netty.NettyClientConnection.ResponseFuture;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import java.util.concurrent.CountDownLatch;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class NettyCloseChannelTest {
  private CountDownLatch _countDownLatch;
  private NettyTCPServer _nettyTCPServer;
  private NettyTCPClientConnection _nettyTCPClientConnection;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _countDownLatch = new CountDownLatch(1);
    NettyTestUtils.LatchControlledRequestHandler requestHandler =
        new NettyTestUtils.LatchControlledRequestHandler(_countDownLatch);
    requestHandler.setResponse(NettyTestUtils.DUMMY_RESPONSE);
    NettyTestUtils.LatchControlledRequestHandlerFactory handlerFactory =
        new NettyTestUtils.LatchControlledRequestHandlerFactory(requestHandler);
    _nettyTCPServer = new NettyTCPServer(NettyTestUtils.DEFAULT_PORT, handlerFactory, null);
    Thread serverThread = new Thread(_nettyTCPServer, "NettyTCPServer");
    serverThread.start();
    // Wait for at most 10 seconds for server to start
    NettyTestUtils.waitForServerStarted(_nettyTCPServer, 10 * 1000L);

    ServerInstance clientServer = new ServerInstance("localhost", NettyTestUtils.DEFAULT_PORT);
    _nettyTCPClientConnection =
        new NettyTCPClientConnection(clientServer, new NioEventLoopGroup(), new HashedWheelTimer(),
            new NettyClientMetrics(null, "abc"));
  }

  /**
   * Client sends a request. Before Server generates the response, client closes the channel.
   */
  @Test
  public void testCloseClientChannel()
      throws Exception {
    Assert.assertTrue(_nettyTCPClientConnection.connect());
    ResponseFuture responseFuture =
        _nettyTCPClientConnection.sendRequest(Unpooled.wrappedBuffer(NettyTestUtils.DUMMY_REQUEST.getBytes()), 1L,
            5000L);

    NettyTestUtils.closeClientConnection(_nettyTCPClientConnection);

    _countDownLatch.countDown();
    byte[] serverResponse = responseFuture.getOne();
    Assert.assertNull(serverResponse);
    Assert.assertFalse(responseFuture.isCancelled());
    Assert.assertNotNull(responseFuture.getError());
  }

  /**
   * Client sends a request. Server closes the channel.
   */
  @Test
  public void testCloseServerChannel()
      throws Exception {
    Assert.assertTrue(_nettyTCPClientConnection.connect());
    ResponseFuture responseFuture =
        _nettyTCPClientConnection.sendRequest(Unpooled.wrappedBuffer(NettyTestUtils.DUMMY_REQUEST.getBytes()), 1L,
            5000L);

    NettyTestUtils.closeServerConnection(_nettyTCPServer);

    _countDownLatch.countDown();
    byte[] serverResponse = responseFuture.getOne();
    Assert.assertNull(serverResponse);
    Assert.assertFalse(responseFuture.isCancelled());
    Assert.assertNotNull(responseFuture.getError());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    NettyTestUtils.closeClientConnection(_nettyTCPClientConnection);
    NettyTestUtils.closeServerConnection(_nettyTCPServer);
  }
}

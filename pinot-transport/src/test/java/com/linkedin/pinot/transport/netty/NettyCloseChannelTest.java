/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class NettyCloseChannelTest {
  private CountDownLatch _countDownLatch;
  private NettyTCPServer _nettyTCPServer;
  private NettyTCPClientConnection _nettyTCPClientConnection;
  private final long RESPONSE_TIMEOUT_IN_SECOND = 10L; // 10 seconds

  @BeforeMethod
  public void setUp()
      throws Exception {
    _countDownLatch = new CountDownLatch(1);
    NettyTestUtils.LatchControlledRequestHandler requestHandler =
        new NettyTestUtils.LatchControlledRequestHandler(_countDownLatch);
    requestHandler.setResponse(NettyTestUtils.DUMMY_RESPONSE);
    NettyTestUtils.LatchControlledRequestHandlerFactory handlerFactory =
        new NettyTestUtils.LatchControlledRequestHandlerFactory(requestHandler);
    _nettyTCPServer = new NettyTCPServer(NettyTestUtils.DEFAULT_PORT, handlerFactory, null, 100, 1, 2);
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
    byte[] serverResponse = null;
    try {
      serverResponse = responseFuture.getOne(RESPONSE_TIMEOUT_IN_SECOND, TimeUnit.SECONDS);
    } catch (TimeoutException te) {
      Assert.fail("Timeout when sending request to server!");
    }
    Assert.assertNull(serverResponse);
    Assert.assertFalse(responseFuture.isCancelled());
    Assert.assertNotNull(responseFuture.getError());
  }

  /**
   * Client sends a request. Server closes the channel.
   * This test sometimes fails only in Travis because the hardware who run this test may not have sufficient cores to be assigned to all the threads at a time.
   * The reason why this test can be ignored is the purpose of this test is to test the response is null when the server connection gets closed.
   * Whereas because of the hardware in Travis, threads in worker group may still get the request and thus cannot be closed in time.
   * The expected behavior is that response is null but it's still acceptable when this response isn't null, i.e. getting a real response.
   * And users won't have any big effect when getting a non-null response. What's more, discussed with Jackie offline, this part of code will be rewritten in the near future.
   * So for now it's ok to just ignore this test.
   */
  @Test(enabled = false)
  public void testCloseServerChannel()
      throws Exception {
    Assert.assertTrue(_nettyTCPClientConnection.connect());
    ResponseFuture responseFuture =
        _nettyTCPClientConnection.sendRequest(Unpooled.wrappedBuffer(NettyTestUtils.DUMMY_REQUEST.getBytes()), 1L,
            5000L);

    NettyTestUtils.closeServerConnection(_nettyTCPServer);

    _countDownLatch.countDown();
    byte[] serverResponse = null;
    try {
      serverResponse = responseFuture.getOne(RESPONSE_TIMEOUT_IN_SECOND, TimeUnit.SECONDS);
    } catch (TimeoutException te) {
      Assert.fail("Timeout when sending request to server!");
    }
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

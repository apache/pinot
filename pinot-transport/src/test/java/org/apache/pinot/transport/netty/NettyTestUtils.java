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
package org.apache.pinot.transport.netty;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;


public class NettyTestUtils {
  private NettyTestUtils() {
  }

  public static final String DUMMY_RESPONSE = "Dummy Response";
  public static final String DUMMY_REQUEST = "Dummy Request";
  public static final int DEFAULT_PORT = 9089;

  public static class LatchControlledRequestHandler implements NettyServer.RequestHandler {
    private final CountDownLatch _responseHandlingLatch;
    private String _response;
    private String _request;

    public LatchControlledRequestHandler(@Nullable CountDownLatch responseHandlingLatch) {
      _responseHandlingLatch = responseHandlingLatch;
    }

    public void setResponse(String response) {
      _response = response;
    }

    @Override
    public ListenableFuture<byte[]> processRequest(byte[] request) {
      _request = new String(request);
      if (_responseHandlingLatch != null) {
        while (true) {
          try {
            _responseHandlingLatch.await();
            break;
          } catch (InterruptedException e) {
            // Ignore
          }
        }
      }
      return Futures.immediateFuture(_response.getBytes());
    }

    public String getRequest() {
      return _request;
    }
  }

  public static class LatchControlledRequestHandlerFactory implements NettyServer.RequestHandlerFactory {
    private final LatchControlledRequestHandler _requestHandler;

    public LatchControlledRequestHandlerFactory(LatchControlledRequestHandler requestHandler) {
      _requestHandler = requestHandler;
    }

    @Override
    public NettyServer.RequestHandler createNewRequestHandler() {
      return _requestHandler;
    }
  }

  public static void waitForServerStarted(NettyTCPServer server, long timeOutInMillis)
      throws TimeoutException {
    long endTime = System.currentTimeMillis() + timeOutInMillis;
    while (System.currentTimeMillis() < endTime) {
      if (server.isStarted()) {
        return;
      }
      Uninterruptibles.sleepUninterruptibly(10L, TimeUnit.MILLISECONDS);
    }
    throw new TimeoutException("Failed to start server in " + timeOutInMillis + "ms");
  }

  public static void closeClientConnection(NettyTCPClientConnection clientConnection)
      throws InterruptedException {
    clientConnection.close();
  }

  public static void closeServerConnection(NettyTCPServer server) {
    // Wait for at most 1 minute to shutdown the server completely
    server.waitForShutdown(60 * 1000L);
  }
}

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
package org.apache.pinot.controller.api;

import java.io.IOException;
import java.net.ServerSocket;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestHttpServerMock is a generic HttpServer Mock for testing Pinot Rest endpoint behavior.
 * Usage: Construct a {@link HttpHandler} then register it to the path using {@link
 * TestHttpServerMock#start(String, HttpHandler)}.
 * This class will automatically open a port and you can get the Endpoint from the call: {@link
 * TestHttpServerMock#getEndpoint()}
 */
class TestHttpServerMock {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestHttpServerMock.class);

  private HttpServer _httpServer;
  private String _endpoint;

  public void start(String path, HttpHandler handler)
      throws IOException {
    int port = findRandomOpenPort();
    _httpServer = HttpServer.createSimpleServer(null, port);
    _httpServer.getServerConfiguration().addHttpHandler(handler, path);
    new Thread(() -> {
      try {
        _httpServer.start();
      } catch (IOException e) {
        LOGGER.error("Got error starting http server", e);
        throw new RuntimeException(e);
      }
    }).start();
    _endpoint = "http://localhost:" + port;
  }

  public void stop() {
    _httpServer.shutdownNow();
  }

  private int findRandomOpenPort()
      throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  public String getEndpoint() {
    return _endpoint;
  }
}

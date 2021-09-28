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
package org.apache.pinot.controller.utils;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Fake HTTP Server that encapsulates one fake handler.
 */
public class FakeHttpServerUtils {

  private FakeHttpServerUtils() {
  }

  public static class FakeHttpServer {
    public String _endpoint;
    public InetSocketAddress _socket = new InetSocketAddress(0);
    public ExecutorService _executorService;
    public HttpServer _httpServer;

    public FakeHttpServer() {
    }

    public void start(String path, HttpHandler handler)
        throws IOException {
      _executorService = Executors.newCachedThreadPool();
      _httpServer = HttpServer.create(_socket, 0);
      _httpServer.setExecutor(_executorService);
      _httpServer.createContext(path, handler);
      new Thread(() -> _httpServer.start()).start();
      _endpoint = "http://localhost:" + _httpServer.getAddress().getPort();
    }

    public void stop() {
      _executorService.shutdown();
    }
  }
}

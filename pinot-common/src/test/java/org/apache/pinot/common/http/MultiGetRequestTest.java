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
package org.apache.pinot.common.http;

import com.google.common.base.Throwables;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class MultiGetRequestTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiGetRequest.class);

  List<HttpServer> servers = new ArrayList<>();
  final int portStart = 14000;
  final String SUCCESS_MSG = "success";
  final String ERROR_MSG = "error";
  final String TIMEOUT_MSG = "Timeout";
  final int SUCCESS_CODE = 200;
  final int ERROR_CODE = 500;
  final String URI_PATH = "/foo";
  final int TIMEOUT_MS = 5000;

  @BeforeTest
  public void setUpTest()
      throws IOException {

    startServer(portStart, createHandler(SUCCESS_CODE, SUCCESS_MSG, 0));
    startServer(portStart + 1, createHandler(ERROR_CODE, ERROR_MSG, 0));
    startServer(portStart + 2, createHandler(SUCCESS_CODE, TIMEOUT_MSG, TIMEOUT_MS));
  }

  @AfterTest
  public void tearDownTest() {
    for (HttpServer server : servers) {
      server.stop(0);
    }
  }

  private HttpHandler createHandler(final int status, final String msg, final int sleepTimeMs) {
    return new HttpHandler() {
      @Override
      public void handle(HttpExchange httpExchange)
          throws IOException {
        if (sleepTimeMs > 0) {
          try {
            Thread.sleep(sleepTimeMs);
          } catch (InterruptedException e) {
            LOGGER.info("Handler interrupted during sleep");
          }
        }
        httpExchange.sendResponseHeaders(status, msg.length());
        OutputStream responseBody = httpExchange.getResponseBody();
        responseBody.write(msg.getBytes());
        responseBody.close();
      }
    };
  }

  private HttpServer startServer(int port, HttpHandler handler)
      throws IOException {
    final HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(URI_PATH, handler);
    new Thread(new Runnable() {
      @Override
      public void run() {
        server.start();
      }
    }).start();
    servers.add(server);
    return server;
  }

  @Test
  public void testMultiGet() {
    MultiGetRequest mget =
        new MultiGetRequest(Executors.newCachedThreadPool(), new MultiThreadedHttpConnectionManager());
    List<String> urls = Arrays.asList("http://localhost:" + String.valueOf(portStart) + URI_PATH,
        "http://localhost:" + String.valueOf(portStart + 1) + URI_PATH,
        "http://localhost:" + String.valueOf(portStart + 2) + URI_PATH,
        // 2nd request to the same server
        "http://localhost:" + String.valueOf(portStart) + URI_PATH);
    // timeout value needs to be less than 5000ms set above for
    // third server
    final int requestTimeoutMs = 1000;
    CompletionService<GetMethod> completionService = mget.execute(urls, requestTimeoutMs);
    int success = 0;
    int errors = 0;
    int timeouts = 0;
    for (int i = 0; i < urls.size(); i++) {
      GetMethod getMethod = null;
      try {
        getMethod = completionService.take().get();
        if (getMethod.getStatusCode() >= 300) {
          ++errors;
          Assert.assertEquals(getMethod.getResponseBodyAsString(), ERROR_MSG);
        } else {
          ++success;
          Assert.assertEquals(getMethod.getResponseBodyAsString(), SUCCESS_MSG);
        }
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted", e);
        ++errors;
      } catch (ExecutionException e) {
        if (Throwables.getRootCause(e) instanceof SocketTimeoutException) {
          LOGGER.debug("Timeout");
          ++timeouts;
        } else {
          LOGGER.error("Error", e);
          ++errors;
        }
      } catch (IOException e) {
        ++errors;
      } finally {
        if (getMethod != null) {
          getMethod.releaseConnection();
        }
      }
    }

    Assert.assertEquals(success, 2);
    Assert.assertEquals(errors, 1);
    Assert.assertEquals(timeouts, 1);
  }
}

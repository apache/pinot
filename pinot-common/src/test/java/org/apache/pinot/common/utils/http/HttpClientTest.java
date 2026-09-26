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
package org.apache.pinot.common.utils.http;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class HttpClientTest {
  @Test
  public void testRedirectsCanBeDisabled()
      throws Exception {
    assertTrue(HttpClientConfig.DEFAULT_HTTP_CLIENT_CONFIG.isFollowRedirects());
    AtomicInteger targetRequests = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/redirect", exchange -> {
      exchange.getResponseHeaders().add("Location", "/target");
      exchange.sendResponseHeaders(302, -1);
      exchange.close();
    });
    server.createContext("/target", exchange -> {
      targetRequests.incrementAndGet();
      exchange.sendResponseHeaders(200, -1);
      exchange.close();
    });
    server.start();

    HttpClientConfig config = HttpClientConfig.newBuilder().withFollowRedirects(false).build();
    try (HttpClient client = new HttpClient(config, null)) {
      URI uri = URI.create("http://localhost:" + server.getAddress().getPort() + "/redirect");
      SimpleHttpResponse response = client.sendGetRequest(uri);
      assertEquals(response.getStatusCode(), 302);
      assertEquals(targetRequests.get(), 0);
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testResponseLengthCanBeBounded()
      throws Exception {
    String oversizedBody = "x".repeat(4_096);
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/success", exchange -> {
      exchange.sendResponseHeaders(200, oversizedBody.length());
      exchange.getResponseBody().write(oversizedBody.getBytes(UTF_8));
      exchange.close();
    });
    server.createContext("/error", exchange -> {
      exchange.sendResponseHeaders(503, oversizedBody.length());
      exchange.getResponseBody().write(oversizedBody.getBytes(UTF_8));
      exchange.close();
    });
    server.start();

    try (HttpClient client = new HttpClient(HttpClientConfig.DEFAULT_HTTP_CLIENT_CONFIG, null)) {
      String baseUrl = "http://localhost:" + server.getAddress().getPort();
      SimpleHttpResponse success = client.sendGetRequest(URI.create(baseUrl + "/success"), List.of(), 5_000L,
          5_000L, 32);
      assertEquals(success.getResponse(), "x".repeat(32));

      SimpleHttpResponse error = client.sendGetRequest(URI.create(baseUrl + "/error"), List.of(), 5_000L,
          5_000L, 32);
      assertEquals(error.getStatusCode(), 503);
      assertTrue(error.getResponse().contains("x".repeat(32)));
      assertFalse(error.getResponse().contains("x".repeat(33)));
      assertTrue(error.getResponse().length() < 512);
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testBoundedResponseDoesNotDrainChunkedRemainder()
      throws Exception {
    CountDownLatch releaseRemainder = new CountDownLatch(1);
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/slow", exchange -> {
      exchange.sendResponseHeaders(200, 0);
      exchange.getResponseBody().write("x".repeat(64).getBytes(UTF_8));
      exchange.getResponseBody().flush();
      try {
        releaseRemainder.await(10, TimeUnit.SECONDS);
        exchange.getResponseBody().write("y".repeat(64).getBytes(UTF_8));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        exchange.close();
      }
    });
    server.start();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (HttpClient client = new HttpClient(HttpClientConfig.DEFAULT_HTTP_CLIENT_CONFIG, null)) {
      URI uri = URI.create("http://localhost:" + server.getAddress().getPort() + "/slow");
      Future<SimpleHttpResponse> responseFuture = executor.submit(
          () -> client.sendGetRequest(uri, List.of(), 5_000L, 5_000L, 32));
      SimpleHttpResponse response = responseFuture.get(2, TimeUnit.SECONDS);
      assertEquals(response.getResponse(), "x".repeat(32));
    } finally {
      releaseRemainder.countDown();
      executor.shutdownNow();
      server.stop(0);
    }
  }
}

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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
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
}

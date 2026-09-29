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
package org.apache.pinot.plugin.minion.tasks;

import com.google.common.collect.ImmutableSet;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SegmentConversionUtilsTest {
  private static final String TEST_TABLE_WITHOUT_TYPE = "myTable";
  private static final String TEST_TABLE_TYPE = "REALTIME";
  private static final String TEST_TABLE_SEGMENT_1 = "myTable_REALTIME_segment_1";
  private static final String TEST_TABLE_SEGMENT_2 = "myTable_REALTIME_segment_2";
  private static final String TABLE_NAME_WITH_TYPE = "table_OFFLINE";
  private static final String SEGMENT_NAME = "segment";
  private static final String SEGMENT_CRC = "123";
  private static final SegmentZKMetadataCustomMapModifier MODIFIER =
      new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
          Map.of("key", "value"));

  @Test
  public void testGetSegmentNamesForTable()
      throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/segments/myTable", exchange -> {
      byte[] response = JsonUtils.objectToString(
          List.of(Map.of(TEST_TABLE_TYPE, List.of(TEST_TABLE_SEGMENT_1, TEST_TABLE_SEGMENT_2))))
          .getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
      exchange.getResponseBody().write(response);
      exchange.close();
    });
    server.start();
    try {
      Assert.assertEquals(SegmentConversionUtils.getSegmentNamesForTable(
              TEST_TABLE_WITHOUT_TYPE + "_" + TEST_TABLE_TYPE,
              FileUploadDownloadClient.getURI("http", "127.0.0.1", server.getAddress().getPort()), null),
          ImmutableSet.of(TEST_TABLE_SEGMENT_1, TEST_TABLE_SEGMENT_2));
    } finally {
      server.stop(0);
    }
  }

  @Test(dataProvider = "retryableStatusCodes")
  public void testUpdateSegmentZKMetadataRetriesTransientResponse(int transientStatus)
      throws Exception {
    AtomicInteger requestCount = new AtomicInteger();
    HttpServer server = startServer(exchange -> {
      verifyRequest(exchange);
      int status = requestCount.getAndIncrement() == 0 ? transientStatus : HttpStatus.SC_OK;
      sendResponse(exchange, status);
    });
    try {
      SegmentConversionUtils.updateSegmentZKMetadata(retryConfigs(2, 1_000), TABLE_NAME_WITH_TYPE, SEGMENT_NAME,
          serverUrl(server), SEGMENT_CRC, MODIFIER, null);
      Assert.assertEquals(requestCount.get(), 2);
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testUpdateSegmentZKMetadataDoesNotRetryUnavailableApi()
      throws Exception {
    AtomicInteger requestCount = new AtomicInteger();
    HttpServer server = startServer(exchange -> {
      requestCount.incrementAndGet();
      sendResponse(exchange, HttpStatus.SC_NOT_FOUND);
    });
    try {
      HttpErrorStatusException exception = Assert.expectThrows(HttpErrorStatusException.class,
          () -> SegmentConversionUtils.updateSegmentZKMetadata(retryConfigs(2, 1_000), TABLE_NAME_WITH_TYPE,
              SEGMENT_NAME, serverUrl(server), SEGMENT_CRC, MODIFIER, null));
      Assert.assertEquals(exception.getStatusCode(), HttpStatus.SC_NOT_FOUND);
      Assert.assertEquals(requestCount.get(), 1);
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testUpdateSegmentZKMetadataUsesConfiguredTimeout()
      throws Exception {
    AtomicInteger requestCount = new AtomicInteger();
    HttpServer server = startServer(exchange -> {
      requestCount.incrementAndGet();
      try {
        Thread.sleep(250L);
        sendResponse(exchange, HttpStatus.SC_OK);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (IOException ignored) {
        // The client is expected to close the exchange after its configured timeout.
      }
    });
    try {
      Assert.expectThrows(AttemptsExceededException.class,
          () -> SegmentConversionUtils.updateSegmentZKMetadata(retryConfigs(1, 50), TABLE_NAME_WITH_TYPE,
              SEGMENT_NAME, serverUrl(server), SEGMENT_CRC, MODIFIER, null));
      Assert.assertEquals(requestCount.get(), 1);
    } finally {
      server.stop(0);
    }
  }

  @DataProvider(name = "retryableStatusCodes")
  public Object[][] retryableStatusCodes() {
    return new Object[][]{{HttpStatus.SC_CONFLICT}, {HttpStatus.SC_INTERNAL_SERVER_ERROR}};
  }

  private static Map<String, String> retryConfigs(int maxAttempts, int timeoutMs) {
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.MAX_NUM_ATTEMPTS_KEY, Integer.toString(maxAttempts));
    configs.put(MinionConstants.INITIAL_RETRY_DELAY_MS_KEY, "0");
    configs.put(MinionConstants.RETRY_SCALE_FACTOR_KEY, "1");
    configs.put(MinionConstants.SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS_KEY, Integer.toString(timeoutMs));
    return configs;
  }

  private static HttpServer startServer(com.sun.net.httpserver.HttpHandler handler)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/segments", handler);
    server.start();
    return server;
  }

  private static String serverUrl(HttpServer server) {
    return "http://127.0.0.1:" + server.getAddress().getPort() + "/segments";
  }

  private static void verifyRequest(HttpExchange exchange)
      throws IOException {
    Assert.assertEquals(exchange.getRequestMethod(), "PUT");
    Assert.assertEquals(exchange.getRequestURI().getPath(),
        "/segments/" + TABLE_NAME_WITH_TYPE + "/" + SEGMENT_NAME + "/metadata");
    Assert.assertEquals(exchange.getRequestHeaders().getFirst(HttpHeaders.IF_MATCH), SEGMENT_CRC);
    Assert.assertEquals(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8),
        MODIFIER.toJsonString());
  }

  private static void sendResponse(HttpExchange exchange, int statusCode)
      throws IOException {
    byte[] response = "response".getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(statusCode, response.length);
    exchange.getResponseBody().write(response);
    exchange.close();
  }
}

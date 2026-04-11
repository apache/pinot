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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.controller.api.resources.ServerTableSizeReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests that ServerTableSizeReader correctly deserializes SegmentSizeInfo with compression stats fields
 * (rawForwardIndexSizeBytes, compressedForwardIndexSizeBytes, tier, columnCompressionStats).
 */
public class ServerTableSizeReaderRawBytesTest {
  private static final String URI_PATH = "/table/";
  private static final int TIMEOUT_MSEC = 5000;
  private static final int PORT_WITH_STATS = 11100;
  private static final int PORT_WITHOUT_STATS = 11101;
  private static final int PORT_ERROR = 11102;

  private final ExecutorService _executor = Executors.newFixedThreadPool(2);
  private final PoolingHttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
  private HttpServer _serverWithStats;
  private HttpServer _serverWithoutStats;
  private HttpServer _serverError;

  @BeforeClass
  public void setUp()
      throws IOException {
    // Server with compression stats
    Map<String, ColumnCompressionStatsInfo> colStats = new HashMap<>();
    colStats.put("col_a", new ColumnCompressionStatsInfo("col_a", 10000, 2000, 5.0, "LZ4", false,
        List.of("forward_index")));
    colStats.put("col_b", new ColumnCompressionStatsInfo("col_b", 20000, 5000, 4.0, "ZSTANDARD", false,
        List.of("forward_index")));

    List<SegmentSizeInfo> statsSegments = Arrays.asList(
        new SegmentSizeInfo("s1", 50000, 30000, 7000, "default", colStats),
        new SegmentSizeInfo("s2", 40000, 15000, 3000, "tier1", null));
    TableSizeInfo statsTable = new TableSizeInfo("testTable", 90000, statsSegments);

    _serverWithStats = startServer(PORT_WITH_STATS, createHandler(200, statsTable));

    // Server without compression stats (backward compat)
    List<SegmentSizeInfo> noStatsSegments = Arrays.asList(new SegmentSizeInfo("s3", 60000));
    TableSizeInfo noStatsTable = new TableSizeInfo("testTable", 60000, noStatsSegments);
    _serverWithoutStats = startServer(PORT_WITHOUT_STATS, createHandler(200, noStatsTable));

    // Server returning 500
    _serverError = startServer(PORT_ERROR, createHandler(500, null));
  }

  @AfterClass
  public void tearDown() {
    if (_serverWithStats != null) {
      _serverWithStats.stop(0);
    }
    if (_serverWithoutStats != null) {
      _serverWithoutStats.stop(0);
    }
    if (_serverError != null) {
      _serverError.stop(0);
    }
  }

  @Test
  public void testDeserializesNewFields() {
    ServerTableSizeReader reader = new ServerTableSizeReader(_executor, _connectionManager);
    BiMap<String, String> endpoints = HashBiMap.create();
    endpoints.put("server0", "http://localhost:" + PORT_WITH_STATS);

    Map<String, List<SegmentSizeInfo>> result =
        reader.getSegmentSizeInfoFromServers(endpoints, "testTable", TIMEOUT_MSEC);
    assertEquals(result.size(), 1);

    List<SegmentSizeInfo> segments = result.get("server0");
    assertNotNull(segments);
    assertEquals(segments.size(), 2);

    // s1 has compression stats
    SegmentSizeInfo s1 = segments.get(0);
    assertEquals(s1.getSegmentName(), "s1");
    assertEquals(s1.getDiskSizeInBytes(), 50000);
    assertEquals(s1.getRawForwardIndexSizeBytes(), 30000);
    assertEquals(s1.getCompressedForwardIndexSizeBytes(), 7000);
    assertEquals(s1.getTier(), "default");

    Map<String, ColumnCompressionStatsInfo> colStats = s1.getColumnCompressionStats();
    assertNotNull(colStats);
    assertEquals(colStats.size(), 2);
    assertEquals(colStats.get("col_a").getColumn(), "col_a");
    assertEquals(colStats.get("col_a").getUncompressedSizeInBytes(), 10000);
    assertEquals(colStats.get("col_a").getCompressedSizeInBytes(), 2000);
    assertEquals(colStats.get("col_a").getCompressionRatio(), 5.0, 0.01);
    assertEquals(colStats.get("col_a").getCodec(), "LZ4");
    assertFalse(colStats.get("col_a").isHasDictionary());

    // s2 has tier but no column stats
    SegmentSizeInfo s2 = segments.get(1);
    assertEquals(s2.getTier(), "tier1");
    assertEquals(s2.getRawForwardIndexSizeBytes(), 15000);
  }

  @Test
  public void testBackwardCompatWithoutNewFields() {
    ServerTableSizeReader reader = new ServerTableSizeReader(_executor, _connectionManager);
    BiMap<String, String> endpoints = HashBiMap.create();
    endpoints.put("server1", "http://localhost:" + PORT_WITHOUT_STATS);

    Map<String, List<SegmentSizeInfo>> result =
        reader.getSegmentSizeInfoFromServers(endpoints, "testTable", TIMEOUT_MSEC);
    assertEquals(result.size(), 1);

    List<SegmentSizeInfo> segments = result.get("server1");
    assertNotNull(segments);
    assertEquals(segments.size(), 1);

    SegmentSizeInfo s3 = segments.get(0);
    assertEquals(s3.getSegmentName(), "s3");
    assertEquals(s3.getDiskSizeInBytes(), 60000);
    // Default values for missing fields (-1 indicates not available)
    assertEquals(s3.getRawForwardIndexSizeBytes(), -1);
    assertEquals(s3.getCompressedForwardIndexSizeBytes(), -1);
  }

  @Test
  public void testErrorServerExcluded() {
    ServerTableSizeReader reader = new ServerTableSizeReader(_executor, _connectionManager);
    BiMap<String, String> endpoints = HashBiMap.create();
    endpoints.put("server0", "http://localhost:" + PORT_WITH_STATS);
    endpoints.put("server_err", "http://localhost:" + PORT_ERROR);

    Map<String, List<SegmentSizeInfo>> result =
        reader.getSegmentSizeInfoFromServers(endpoints, "testTable", TIMEOUT_MSEC);
    // Error server should be excluded
    assertTrue(result.containsKey("server0"));
    assertFalse(result.containsKey("server_err"));
  }

  private HttpHandler createHandler(int status, TableSizeInfo tableSize) {
    return httpExchange -> {
      String json = tableSize != null ? JsonUtils.objectToString(tableSize) : "error";
      httpExchange.sendResponseHeaders(status, json.length());
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(json.getBytes());
      responseBody.close();
    };
  }

  private HttpServer startServer(int port, HttpHandler handler)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(URI_PATH, handler);
    new Thread(server::start).start();
    return server;
  }
}

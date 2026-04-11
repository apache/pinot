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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.common.restlet.resources.TableMetadataInfo;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests that per-column compression stats are correctly aggregated across servers in the metadata endpoint
 * (ServerSegmentMetadataReader.getAggregatedTableMetadataFromServer).
 */
public class TableMetadataReaderCompressionTest {
  private static final int PORT_SERVER0 = 11200;
  private static final int PORT_SERVER1 = 11201;
  private static final int TIMEOUT_MSEC = 10000;
  private static final int NUM_REPLICAS = 2;

  private final ExecutorService _executor = Executors.newFixedThreadPool(2);
  private final PoolingHttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
  private HttpServer _httpServer0;
  private HttpServer _httpServer1;

  @BeforeClass
  public void setUp()
      throws IOException {
    // Server 0: has compression stats for col_a and col_b
    List<ColumnCompressionStatsInfo> colStats0 = new ArrayList<>();
    colStats0.add(new ColumnCompressionStatsInfo("col_a", 10000, 2000, 5.0, "LZ4", false,
        List.of("forward_index")));
    colStats0.add(new ColumnCompressionStatsInfo("col_b", 20000, 5000, 4.0, "ZSTANDARD", false,
        List.of("forward_index", "inverted_index")));

    TableMetadataInfo server0Info = new TableMetadataInfo("testTable_OFFLINE", 50000, 3, 1000,
        Map.of("col_a", 4.0, "col_b", 100.0),
        Map.of("col_a", 50.0, "col_b", 200.0),
        Map.of(), Map.of(), Map.of(), colStats0);

    _httpServer0 = startServer(PORT_SERVER0, createHandler(server0Info));

    // Server 1 (replica): same compression stats
    List<ColumnCompressionStatsInfo> colStats1 = new ArrayList<>();
    colStats1.add(new ColumnCompressionStatsInfo("col_a", 10000, 2000, 5.0, "LZ4", false,
        List.of("forward_index")));
    colStats1.add(new ColumnCompressionStatsInfo("col_b", 20000, 5000, 4.0, "ZSTANDARD", false,
        List.of("forward_index", "inverted_index")));

    TableMetadataInfo server1Info = new TableMetadataInfo("testTable_OFFLINE", 50000, 3, 1000,
        Map.of("col_a", 4.0, "col_b", 100.0),
        Map.of("col_a", 50.0, "col_b", 200.0),
        Map.of(), Map.of(), Map.of(), colStats1);

    _httpServer1 = startServer(PORT_SERVER1, createHandler(server1Info));
  }

  @AfterClass
  public void tearDown() {
    if (_httpServer0 != null) {
      _httpServer0.stop(0);
    }
    if (_httpServer1 != null) {
      _httpServer1.stop(0);
    }
  }

  @Test
  public void testColumnCompressionStatsAggregation() {
    ServerSegmentMetadataReader reader = new ServerSegmentMetadataReader(_executor, _connectionManager);
    BiMap<String, String> endpoints = HashBiMap.create();
    endpoints.put("server0", "http://localhost:" + PORT_SERVER0);
    endpoints.put("server1", "http://localhost:" + PORT_SERVER1);

    TableMetadataInfo result = reader.getAggregatedTableMetadataFromServer(
        "testTable_OFFLINE", endpoints, null, NUM_REPLICAS, TIMEOUT_MSEC, true);

    assertNotNull(result);
    // Disk size divided by replicas: (50000+50000) / 2 = 50000
    assertEquals(result.getDiskSizeInBytes(), 50000);

    // Per-column compression stats should be aggregated and divided by replicas
    List<ColumnCompressionStatsInfo> colStats = result.getColumnCompressionStats();
    assertNotNull(colStats);
    assertEquals(colStats.size(), 2);

    // Results are sorted by column name
    ColumnCompressionStatsInfo colA = colStats.get(0);
    assertNotNull(colA);
    assertEquals(colA.getColumn(), "col_a");
    // (10000+10000)/2 = 10000 uncompressed, (2000+2000)/2 = 2000 compressed
    assertEquals(colA.getUncompressedSizeInBytes(), 10000);
    assertEquals(colA.getCompressedSizeInBytes(), 2000);
    assertEquals(colA.getCompressionRatio(), 5.0, 0.01);
    assertEquals(colA.getCodec(), "LZ4");
    assertFalse(colA.isHasDictionary());

    ColumnCompressionStatsInfo colB = colStats.get(1);
    assertNotNull(colB);
    assertEquals(colB.getColumn(), "col_b");
    // (20000+20000)/2 = 20000 uncompressed, (5000+5000)/2 = 5000 compressed
    assertEquals(colB.getUncompressedSizeInBytes(), 20000);
    assertEquals(colB.getCompressedSizeInBytes(), 5000);
    assertEquals(colB.getCompressionRatio(), 4.0, 0.01);
    assertEquals(colB.getCodec(), "ZSTANDARD");
    assertFalse(colB.isHasDictionary());
  }

  @Test
  public void testNoCompressionStatsFromServers() {
    // Server with no compression stats (old server)
    ServerSegmentMetadataReader reader = new ServerSegmentMetadataReader(_executor, _connectionManager);

    // Create a temporary server without compression stats
    HttpServer noStatsServer = null;
    try {
      TableMetadataInfo noStatsInfo = new TableMetadataInfo("testTable_OFFLINE", 30000, 2, 500,
          Map.of("col_a", 4.0), Map.of("col_a", 50.0), Map.of(), Map.of(), Map.of());
      noStatsServer = startServer(11210, createHandler(noStatsInfo));

      BiMap<String, String> endpoints = HashBiMap.create();
      endpoints.put("old_server", "http://localhost:11210");

      TableMetadataInfo result = reader.getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints, null, 1, TIMEOUT_MSEC, true);

      assertNotNull(result);
      // No compression stats should result in null list
      assertNull(result.getColumnCompressionStats());
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (noStatsServer != null) {
        noStatsServer.stop(0);
      }
    }
  }

  @Test
  public void testCompressionStatsSuppressedWhenFlagOff() {
    ServerSegmentMetadataReader reader = new ServerSegmentMetadataReader(_executor, _connectionManager);
    BiMap<String, String> endpoints = HashBiMap.create();
    endpoints.put("server0", "http://localhost:" + PORT_SERVER0);
    endpoints.put("server1", "http://localhost:" + PORT_SERVER1);

    // Flag OFF: compression stats and columnCompressionStats should be null,
    // but storageBreakdown should still be preserved
    TableMetadataInfo result = reader.getAggregatedTableMetadataFromServer(
        "testTable_OFFLINE", endpoints, null, NUM_REPLICAS, TIMEOUT_MSEC, false);

    assertNotNull(result);
    assertNull(result.getColumnCompressionStats(), "columnCompressionStats should be null when flag is OFF");
    assertNull(result.getCompressionStats(), "compressionStats should be null when flag is OFF");
    // storageBreakdown is always-on; it is null here only because test servers don't send it
  }

  private HttpHandler createHandler(TableMetadataInfo info) {
    return httpExchange -> {
      String json = JsonUtils.objectToString(info);
      httpExchange.sendResponseHeaders(200, json.length());
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(json.getBytes());
      responseBody.close();
    };
  }

  private HttpServer startServer(int port, HttpHandler handler)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext("/tables/", handler);
    new Thread(server::start).start();
    return server;
  }
}

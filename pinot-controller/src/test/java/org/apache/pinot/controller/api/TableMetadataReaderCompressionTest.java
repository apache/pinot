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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.common.restlet.resources.CompressionStatsSummary;
import org.apache.pinot.common.restlet.resources.StorageBreakdownInfo;
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
    colStats0.add(new ColumnCompressionStatsInfo("col_a", 10000, 2000, 5.0, "LZ4",
        List.of("forward_index"), null));
    colStats0.add(new ColumnCompressionStatsInfo("col_b", 20000, 5000, 4.0, "ZSTANDARD",
        List.of("forward_index", "inverted_index"), null));

    TableMetadataInfo server0Info = new TableMetadataInfo("testTable_OFFLINE", 50000, 3, 1000,
        Map.of("col_a", 4.0, "col_b", 100.0),
        Map.of("col_a", 50.0, "col_b", 200.0),
        Map.of(), Map.of(), Map.of(), colStats0);

    _httpServer0 = startServer(PORT_SERVER0, createHandler(server0Info));

    // Server 1 (replica): same compression stats
    List<ColumnCompressionStatsInfo> colStats1 = new ArrayList<>();
    colStats1.add(new ColumnCompressionStatsInfo("col_a", 10000, 2000, 5.0, "LZ4",
        List.of("forward_index"), null));
    colStats1.add(new ColumnCompressionStatsInfo("col_b", 20000, 5000, 4.0, "ZSTANDARD",
        List.of("forward_index", "inverted_index"), null));

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
    assertEquals(colA.getRawIngestSizeInBytes(), 10000);
    assertEquals(colA.getOnDiskSizeInBytes(), 2000);
    assertEquals(colA.getCompressionRatio(), 5.0, 0.01);
    assertEquals(colA.getCodec(), "LZ4");

    ColumnCompressionStatsInfo colB = colStats.get(1);
    assertNotNull(colB);
    assertEquals(colB.getColumn(), "col_b");
    // (20000+20000)/2 = 20000 uncompressed, (5000+5000)/2 = 5000 compressed
    assertEquals(colB.getRawIngestSizeInBytes(), 20000);
    assertEquals(colB.getOnDiskSizeInBytes(), 5000);
    assertEquals(colB.getCompressionRatio(), 4.0, 0.01);
    assertEquals(colB.getCodec(), "ZSTANDARD");
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

  @Test
  public void testCompressionSummaryAndStorageBreakdownAggregation()
      throws IOException {
    // Build a server response that includes CompressionStatsSummary and StorageBreakdownInfo
    Map<String, StorageBreakdownInfo.TierInfo> tiers = new HashMap<>();
    tiers.put("default", new StorageBreakdownInfo.TierInfo(3, 150000));
    tiers.put("cold", new StorageBreakdownInfo.TierInfo(1, 60000));
    StorageBreakdownInfo breakdown = new StorageBreakdownInfo(tiers);

    List<ColumnCompressionStatsInfo> colStats = new ArrayList<>();
    colStats.add(new ColumnCompressionStatsInfo("col_a", 20000, 4000, 5.0, "LZ4", null, null));

    CompressionStatsSummary summary = new CompressionStatsSummary(20000, 4000, 5.0, 3, 3, false);

    TableMetadataInfo info = new TableMetadataInfo("testTable_OFFLINE", 200000, 4, 2000,
        Map.of("col_a", 4.0), Map.of("col_a", 50.0),
        Map.of(), Map.of(), Map.of(), colStats, summary, breakdown);

    HttpServer summaryServer = startServer(11215, createHandler(info));
    try {
      ServerSegmentMetadataReader reader = new ServerSegmentMetadataReader(_executor, _connectionManager);
      BiMap<String, String> endpoints = HashBiMap.create();
      endpoints.put("srv", "http://localhost:11215");

      TableMetadataInfo result = reader.getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints, null, 1, TIMEOUT_MSEC, true);

      assertNotNull(result);

      // CompressionStatsSummary should be aggregated and returned
      CompressionStatsSummary resultSummary = result.getCompressionStats();
      assertNotNull(resultSummary, "compressionStats should be aggregated from server response");
      assertEquals(resultSummary.getRawIngestSizePerReplicaInBytes(), 20000);
      assertEquals(resultSummary.getOnDiskSizePerReplicaInBytes(), 4000);
      assertEquals(resultSummary.getCompressionRatio(), 5.0, 0.01);
      assertEquals(resultSummary.getSegmentsWithStats(), 3);
      assertEquals(resultSummary.getTotalSegments(), 3);
      assertFalse(resultSummary.isPartialCoverage());

      // StorageBreakdownInfo should be aggregated and divided by numReplica (1 here)
      StorageBreakdownInfo resultBreakdown = result.getStorageBreakdown();
      assertNotNull(resultBreakdown, "storageBreakdown should be aggregated from server response");
      assertNotNull(resultBreakdown.getTiers());
      assertEquals(resultBreakdown.getTiers().size(), 2);
      StorageBreakdownInfo.TierInfo defaultTier = resultBreakdown.getTiers().get("default");
      assertNotNull(defaultTier);
      assertEquals(defaultTier.getCount(), 3);
      assertEquals(defaultTier.getSizePerReplicaInBytes(), 150000);
    } finally {
      summaryServer.stop(0);
    }
  }

  @Test
  public void testDictColumnSentinelAndSkipPath()
      throws IOException {
    // Dict column: uncompressed=-1 sentinel, codec=CODEC_DICT_ENCODED → preserved
    // Old raw column: uncompressed=0, codec=null → skipped (no stats)
    List<ColumnCompressionStatsInfo> colStats = new ArrayList<>();
    colStats.add(new ColumnCompressionStatsInfo("dict_col", -1, 8000, 0.0,
        ColumnCompressionStatsInfo.CODEC_DICT_ENCODED, List.of("forward_index"), null));
    colStats.add(new ColumnCompressionStatsInfo("old_raw_col", 0, 5000, 0.0, null, null, null));

    TableMetadataInfo info = new TableMetadataInfo("testTable_OFFLINE", 100000, 2, 1000,
        Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), colStats);

    HttpServer server = startServer(11216, createHandler(info));
    try {
      ServerSegmentMetadataReader reader = new ServerSegmentMetadataReader(_executor, _connectionManager);
      BiMap<String, String> endpoints = HashBiMap.create();
      endpoints.put("srv", "http://localhost:11216");

      TableMetadataInfo result = reader.getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints, null, 1, TIMEOUT_MSEC, true);

      assertNotNull(result);
      List<ColumnCompressionStatsInfo> stats = result.getColumnCompressionStats();
      assertNotNull(stats);
      // old_raw_col (codec=null, rawIngest=0) must be skipped
      assertEquals(stats.size(), 1);
      ColumnCompressionStatsInfo dictColInfo = stats.get(0);
      assertEquals(dictColInfo.getColumn(), "dict_col");
      // dict column: sentinel -1 preserved (not divided as 0)
      assertEquals(dictColInfo.getRawIngestSizeInBytes(), -1);
      assertEquals(dictColInfo.getOnDiskSizeInBytes(), 8000);
      assertEquals(dictColInfo.getCodec(), ColumnCompressionStatsInfo.CODEC_DICT_ENCODED);
    } finally {
      server.stop(0);
    }
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

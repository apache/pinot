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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.controller.api.resources.ServerTableSizeReader;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Tests that [ServerTableSizeReader] deserializes [SegmentSizeInfo] compression-statistics fields.
public class ServerTableSizeReaderCompressionStatsTest {
  private static final String URI_PATH = "/table/";
  private static final int TIMEOUT_MSEC = 5000;
  private final ExecutorService _executor = Executors.newFixedThreadPool(2);
  private final PoolingHttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
  private HttpServer _serverWithStats;
  private HttpServer _serverWithoutStats;
  private HttpServer _serverError;
  private int _portWithStats;
  private int _portWithoutStats;
  private int _portError;
  private final AtomicReference<String> _statsQuery = new AtomicReference<>();

  @BeforeClass
  public void setUp()
      throws IOException {
    // Server with compression stats
    Map<String, ColumnCompressionStatsInfo> colStats = new HashMap<>();
    colStats.put("col_a", rawStats("col_a", 10000, 2000, ChunkCompressionType.LZ4));
    colStats.put("col_b", rawStats("col_b", 20000, 5000, ChunkCompressionType.ZSTANDARD));

    List<SegmentSizeInfo> statsSegments = List.of(
        new SegmentSizeInfo("s1", 50000, 30000, 7000, colStats),
        new SegmentSizeInfo("s2", 40000, 15000, 3000));
    TableSizeInfo statsTable =
        new TableSizeInfo("testTable", 90000, statsSegments, TableSizeInfo.CURRENT_METADATA_VERSION);

    _serverWithStats = startServer(createRecordingHandler(200, statsTable, _statsQuery));
    _portWithStats = _serverWithStats.getAddress().getPort();

    // Server without compression stats (backward compat)
    List<SegmentSizeInfo> noStatsSegments = List.of(new SegmentSizeInfo("s3", 60000));
    TableSizeInfo noStatsTable = new TableSizeInfo("testTable", 60000, noStatsSegments);
    _serverWithoutStats = startServer(createHandler(200, noStatsTable));
    _portWithoutStats = _serverWithoutStats.getAddress().getPort();

    // Server returning 500
    _serverError = startServer(createHandler(500, null));
    _portError = _serverError.getAddress().getPort();
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
    _executor.shutdownNow();
    try {
      _executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    _connectionManager.close();
  }

  @Test
  public void testDeserializesNewFields() {
    ServerTableSizeReader reader = new ServerTableSizeReader(_executor, _connectionManager);
    BiMap<String, String> endpoints = HashBiMap.create();
    endpoints.put("server0", "http://localhost:" + _portWithStats);

    Map<String, List<SegmentSizeInfo>> result =
        reader.getSegmentSizeInfoFromServers(endpoints, "testTable", TIMEOUT_MSEC, true);
    assertEquals(result.size(), 1);

    List<SegmentSizeInfo> segments = result.get("server0");
    assertNotNull(segments);
    assertEquals(segments.size(), 2);

    // s1 has compression stats
    SegmentSizeInfo s1 = segments.get(0);
    assertEquals(s1.getSegmentName(), "s1");
    assertEquals(s1.getDiskSizeInBytes(), 50000);
    assertEquals(s1.getCompressionStatsUncompressedValueSizeInBytes(), 30000);
    assertEquals(s1.getCompressionStatsForwardIndexAndDictionaryStorageSizeInBytes(), 7000);
    Map<String, ColumnCompressionStatsInfo> colStats = s1.getColumnCompressionStats();
    assertNotNull(colStats);
    assertEquals(colStats.size(), 2);
    assertEquals(colStats.get("col_a").getColumn(), "col_a");
    assertEquals(colStats.get("col_a").getUncompressedValueSizeInBytes(), 10000);
    assertEquals(colStats.get("col_a").getForwardIndexAndDictionaryStorageSizeInBytes(), 2000);
    assertEquals(colStats.get("col_a").getEncodingBreakdown().get(0).getChunkCompressionType(),
        ChunkCompressionType.LZ4);
    assertTrue(_statsQuery.get().contains("includeCompressionStats=true"));
    assertTrue(_statsQuery.get().contains("includeColumnCompressionStats=true"));

    // s2 has summary stats but no column details.
    SegmentSizeInfo s2 = segments.get(1);
    assertEquals(s2.getCompressionStatsUncompressedValueSizeInBytes(), 15000);
  }

  @Test
  public void testBackwardCompatWithoutNewFields() {
    ServerTableSizeReader reader = new ServerTableSizeReader(_executor, _connectionManager);
    BiMap<String, String> endpoints = HashBiMap.create();
    endpoints.put("server1", "http://localhost:" + _portWithoutStats);

    Map<String, List<SegmentSizeInfo>> result =
        reader.getSegmentSizeInfoFromServers(endpoints, "testTable", TIMEOUT_MSEC, true);
    assertEquals(result.size(), 1);

    List<SegmentSizeInfo> segments = result.get("server1");
    assertNotNull(segments);
    assertEquals(segments.size(), 1);

    SegmentSizeInfo s3 = segments.get(0);
    assertEquals(s3.getSegmentName(), "s3");
    assertEquals(s3.getDiskSizeInBytes(), 60000);
    // Default values for missing fields (-1 indicates not available)
    assertEquals(s3.getCompressionStatsUncompressedValueSizeInBytes(), -1);
    assertEquals(s3.getCompressionStatsForwardIndexAndDictionaryStorageSizeInBytes(), -1);
  }

  @Test
  public void testErrorServerExcluded() {
    ServerTableSizeReader reader = new ServerTableSizeReader(_executor, _connectionManager);
    BiMap<String, String> endpoints = HashBiMap.create();
    endpoints.put("server0", "http://localhost:" + _portWithStats);
    endpoints.put("server_err", "http://localhost:" + _portError);

    Map<String, List<SegmentSizeInfo>> result =
        reader.getSegmentSizeInfoFromServers(endpoints, "testTable", TIMEOUT_MSEC, true);
    // Error server should be excluded
    assertTrue(result.containsKey("server0"));
    assertFalse(result.containsKey("server_err"));
  }

  private HttpHandler createHandler(int status, TableSizeInfo tableSize) {
    return httpExchange -> {
      byte[] json = (tableSize != null ? JsonUtils.objectToString(tableSize) : "error")
          .getBytes(StandardCharsets.UTF_8);
      httpExchange.sendResponseHeaders(status, json.length);
      try (OutputStream responseBody = httpExchange.getResponseBody()) {
        responseBody.write(json);
      }
    };
  }

  private HttpHandler createRecordingHandler(int status, TableSizeInfo tableSize, AtomicReference<String> query) {
    HttpHandler delegate = createHandler(status, tableSize);
    return exchange -> {
      query.set(exchange.getRequestURI().getQuery());
      delegate.handle(exchange);
    };
  }

  private HttpServer startServer(HttpHandler handler)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(URI_PATH, handler);
    server.start();
    return server;
  }

  private static ColumnCompressionStatsInfo rawStats(String column, long rawSize,
      long forwardIndexAndDictionaryStorageSize, ChunkCompressionType chunkCompressionType) {
    return ColumnCompressionStatsInfo.builder(column)
        .withUncompressedValueSizeInBytes(rawSize)
        .withForwardIndexAndDictionaryStorageSizeInBytes(forwardIndexAndDictionaryStorageSize)
        .withCompressionRatio((double) rawSize / forwardIndexAndDictionaryStorageSize)
        .withObservedIndexes(List.of("forward_index"))
        .withEncodingBreakdown(List.of(new ColumnCompressionStatsInfo.EncodingBreakdownEntry(
            EncodingType.RAW, chunkCompressionType, 1, rawSize, forwardIndexAndDictionaryStorageSize)))
        .withNumSegments(1)
        .build();
  }
}

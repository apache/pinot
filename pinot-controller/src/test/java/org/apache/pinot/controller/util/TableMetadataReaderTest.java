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
package org.apache.pinot.controller.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class TableMetadataReaderTest {
  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final int TIMEOUT_MS = 10_000;

  private final ExecutorService _executor = Executors.newFixedThreadPool(4);
  private final PoolingHttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
  private final List<HttpServer> _httpServers = new ArrayList<>();

  @BeforeClass
  public void setUp() {
    _connectionManager.setDefaultMaxPerRoute(4);
  }

  @AfterMethod
  public void stopServers() {
    _httpServers.forEach(server -> server.stop(0));
    _httpServers.clear();
  }

  @AfterClass
  public void tearDown() {
    _executor.shutdownNow();
    _connectionManager.close();
  }

  /// Every server holds every segment, so any single server's response is already complete.
  @Test
  public void testTableLevelMetadataWithFullReplication()
      throws Exception {
    Map<String, List<String>> serverToSegments = new LinkedHashMap<>();
    serverToSegments.put("server0", List.of("seg0", "seg1", "seg2"));
    serverToSegments.put("server1", List.of("seg0", "seg1", "seg2"));
    serverToSegments.put("server2", List.of("seg0", "seg1", "seg2"));

    JsonNode metadata = getSegmentsMetadata(serverToSegments);

    assertEquals(segmentNames(metadata), Set.of("seg0", "seg1", "seg2"));
  }

  /// 4 servers with replication 2: each server holds only half of the segments, so every server's response is needed.
  @Test
  public void testTableLevelMetadataWithPartialReplication()
      throws Exception {
    Map<String, List<String>> serverToSegments = new LinkedHashMap<>();
    serverToSegments.put("server0", List.of("seg0", "seg1"));
    serverToSegments.put("server1", List.of("seg1", "seg2"));
    serverToSegments.put("server2", List.of("seg2", "seg3"));
    serverToSegments.put("server3", List.of("seg3", "seg0"));

    JsonNode metadata = getSegmentsMetadata(serverToSegments);

    assertEquals(segmentNames(metadata), Set.of("seg0", "seg1", "seg2", "seg3"));
  }

  /// Each server holds a distinct segment, so dropping any server's response loses its segment.
  @Test
  public void testTableLevelMetadataWithoutReplication()
      throws Exception {
    Map<String, List<String>> serverToSegments = new LinkedHashMap<>();
    serverToSegments.put("server0", List.of("seg0"));
    serverToSegments.put("server1", List.of("seg1"));
    serverToSegments.put("server2", List.of("seg2"));

    JsonNode metadata = getSegmentsMetadata(serverToSegments);

    assertEquals(segmentNames(metadata), Set.of("seg0", "seg1", "seg2"));
  }

  /// A server whose table-level request fails must fail the whole call instead of returning partial metadata.
  @Test
  public void testTableLevelMetadataFailsWhenServerReturnsError() {
    Map<String, List<String>> serverToSegments = new LinkedHashMap<>();
    serverToSegments.put("server0", List.of("seg0"));
    serverToSegments.put("server1", List.of("seg1"));

    assertThrows(RuntimeException.class, () -> getSegmentsMetadata(serverToSegments, Set.of("server1")));
  }

  /// 3 servers x 3 segments with full replication is 9 entries without columns: at the limit the call goes through.
  @Test
  public void testSegmentsMetadataAtEntryLimit()
      throws Exception {
    JsonNode metadata = getSegmentsMetadata(fullyReplicated(), Set.of(), null, 9);

    assertEquals(segmentNames(metadata), Set.of("seg0", "seg1", "seg2"));
  }

  /// One entry over the limit is rejected before any server is contacted.
  @Test
  public void testSegmentsMetadataOverEntryLimitIsRejected()
      throws Exception {
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    when(helixResourceManager.getServerToSegmentsMap(TABLE_NAME)).thenReturn(fullyReplicated());
    TableMetadataReader reader = new TableMetadataReader(_executor, _connectionManager, helixResourceManager);

    assertThrows(IllegalArgumentException.class,
        () -> reader.getSegmentsMetadata(TABLE_NAME, null, null, TIMEOUT_MS, 8));
    verify(helixResourceManager, never()).getDataInstanceAdminEndpoints(anySet());
  }

  /// `*` counts every schema column: 1 segment x 3 replicas x (1 + 4 columns) = 15 entries, one over a limit of 14.
  @Test
  public void testSegmentsMetadataStarCountsSchemaColumns() {
    TableMetadataReader reader = readerWithSchema(fourColumnSchema());

    assertThrows(IllegalArgumentException.class,
        () -> reader.getSegmentsMetadata(TABLE_NAME, List.of("*"), List.of("seg0"), TIMEOUT_MS, 14));
  }

  /// 1 segment x 3 replicas: segment metadata counts as one entry per replica, plus one per requested column.
  @DataProvider
  public Object[][] entryLimitBoundaries() {
    return new Object[][]{
        {null, 3},
        {List.of("c0"), 6},
        {List.of("c0", "c1", "c2", "c3", "c4"), 18}
    };
  }

  /// At the limit the call goes through; one entry under it is rejected.
  @Test(dataProvider = "entryLimitBoundaries")
  public void testSegmentsMetadataEntryLimitBoundary(@Nullable List<String> columns, int numEntries)
      throws Exception {
    Map<String, List<String>> serverToSegments = new LinkedHashMap<>();
    serverToSegments.put("server0", List.of("seg0"));
    serverToSegments.put("server1", List.of("seg0"));
    serverToSegments.put("server2", List.of("seg0"));

    JsonNode metadata = getSegmentsMetadata(serverToSegments, Set.of(), columns, numEntries);

    assertEquals(segmentNames(metadata), Set.of("seg0"));
    assertThrows(IllegalArgumentException.class,
        () -> getSegmentsMetadata(serverToSegments, Set.of(), columns, numEntries - 1));
  }

  @Test
  public void testGetNumColumns()
      throws Exception {
    TableMetadataReader reader = readerWithSchema(fourColumnSchema());

    // No columns (null from internal callers, empty list from the REST endpoint): segment-level metadata only
    assertEquals(reader.getNumColumns(TABLE_NAME, null), 0);
    assertEquals(reader.getNumColumns(TABLE_NAME, List.of()), 0);
    // Explicit columns
    assertEquals(reader.getNumColumns(TABLE_NAME, List.of("c0", "c1")), 2);
    // `*` anywhere in the list means every schema column
    assertEquals(reader.getNumColumns(TABLE_NAME, List.of("*")), 4);
    assertEquals(reader.getNumColumns(TABLE_NAME, List.of("c0", "*")), 4);
  }

  @Test
  public void testGetNumColumnsStarWithoutSchema() {
    TableMetadataReader reader = readerWithSchema(null);

    assertThrows(InvalidConfigException.class, () -> reader.getNumColumns(TABLE_NAME, List.of("*")));
  }

  private TableMetadataReader readerWithSchema(@Nullable Schema schema) {
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    when(helixResourceManager.getServerToSegmentsMap(TABLE_NAME)).thenReturn(fullyReplicated());
    when(helixResourceManager.getTableSchema(TABLE_NAME)).thenReturn(schema);
    return new TableMetadataReader(_executor, _connectionManager, helixResourceManager);
  }

  private static Schema fourColumnSchema() {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension("c0", DataType.INT)
        .addSingleValueDimension("c1", DataType.INT)
        .addSingleValueDimension("c2", DataType.INT)
        .addSingleValueDimension("c3", DataType.INT)
        .build();
  }

  @Test
  public void testCountSegmentReplicas() {
    Map<String, List<String>> serverToSegments = Map.of("server0", List.of("seg0", "seg1"), "server1",
        List.of("seg1", "seg2"));

    // All hosted segments, replicas included
    assertEquals(TableMetadataReader.countSegmentReplicas(serverToSegments, null), 4);
    assertEquals(TableMetadataReader.countSegmentReplicas(serverToSegments, List.of()), 4);
    // Only requested segments; unhosted ones count nothing
    assertEquals(TableMetadataReader.countSegmentReplicas(serverToSegments, List.of("seg1", "missing")), 2);
  }

  private static Map<String, List<String>> fullyReplicated() {
    Map<String, List<String>> serverToSegments = new LinkedHashMap<>();
    serverToSegments.put("server0", List.of("seg0", "seg1", "seg2"));
    serverToSegments.put("server1", List.of("seg0", "seg1", "seg2"));
    serverToSegments.put("server2", List.of("seg0", "seg1", "seg2"));
    return serverToSegments;
  }

  private JsonNode getSegmentsMetadata(Map<String, List<String>> serverToSegments)
      throws Exception {
    return getSegmentsMetadata(serverToSegments, Set.of());
  }

  private JsonNode getSegmentsMetadata(Map<String, List<String>> serverToSegments, Set<String> failingServers)
      throws Exception {
    return getSegmentsMetadata(serverToSegments, failingServers, null,
        ControllerConf.DEFAULT_SEGMENT_METADATA_MAX_ENTRY_LIMIT);
  }

  private JsonNode getSegmentsMetadata(Map<String, List<String>> serverToSegments, Set<String> failingServers,
      @Nullable List<String> columns, int maxEntries)
      throws Exception {
    BiMap<String, String> serverToEndpoint = HashBiMap.create();
    for (Map.Entry<String, List<String>> entry : serverToSegments.entrySet()) {
      HttpServer httpServer =
          failingServers.contains(entry.getKey()) ? startFailingServer() : startServer(entry.getValue());
      serverToEndpoint.put(entry.getKey(), "http://localhost:" + httpServer.getAddress().getPort());
    }

    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    when(helixResourceManager.getServerToSegmentsMap(TABLE_NAME)).thenReturn(serverToSegments);
    when(helixResourceManager.getDataInstanceAdminEndpoints(anySet())).thenReturn(serverToEndpoint);

    TableMetadataReader reader = new TableMetadataReader(_executor, _connectionManager, helixResourceManager);
    return reader.getSegmentsMetadata(TABLE_NAME, columns, null, TIMEOUT_MS, maxEntries);
  }

  /// Starts a fake server whose table-level endpoint returns metadata for the given segments.
  private HttpServer startServer(List<String> segments)
      throws IOException {
    ObjectNode tableLevelResponse = JsonUtils.newObjectNode();
    for (String segment : segments) {
      tableLevelResponse.set(segment, JsonUtils.newObjectNode().put("segmentName", segment));
    }
    byte[] tableLevelBody = JsonUtils.objectToString(tableLevelResponse).getBytes(StandardCharsets.UTF_8);

    HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    httpServer.createContext("/tables/" + TABLE_NAME + "/segments/metadata", exchange -> {
      exchange.sendResponseHeaders(200, tableLevelBody.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(tableLevelBody);
      }
    });
    httpServer.start();
    _httpServers.add(httpServer);
    return httpServer;
  }

  /// Starts a fake server whose table-level endpoint always returns 500.
  private HttpServer startFailingServer()
      throws IOException {
    HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    httpServer.createContext("/tables/" + TABLE_NAME + "/segments/metadata", exchange -> {
      exchange.sendResponseHeaders(500, -1);
      exchange.close();
    });
    httpServer.start();
    _httpServers.add(httpServer);
    return httpServer;
  }

  /// Preserves per-replica CRCs across servers, including when replicas of the same segment diverge mid-refresh.
  @Test
  public void testGetSegmentCrcsAcrossReplicas()
      throws Exception {
    HttpServer s0 = startCrcServer(TABLE_NAME, 200,
        JsonUtils.objectToString(Map.of("seg_A", "111", "seg_B", "222")), null);
    HttpServer s1 = startCrcServer(TABLE_NAME, 200,
        JsonUtils.objectToString(Map.of("seg_A", "999")), null);
    HttpServer s2 = startCrcServer(TABLE_NAME, 200,
        JsonUtils.objectToString(Map.of("seg_B", "222")), null);

    Map<String, List<String>> serverToSegments = Map.of(
        "Server_host0_8098", List.of("seg_A", "seg_B"),
        "Server_host1_8098", List.of("seg_A"),
        "Server_host2_8098", List.of("seg_B"));
    BiMap<String, String> endpoints = HashBiMap.create();
    endpoints.put("Server_host0_8098", "http://localhost:" + s0.getAddress().getPort());
    endpoints.put("Server_host1_8098", "http://localhost:" + s1.getAddress().getPort());
    endpoints.put("Server_host2_8098", "http://localhost:" + s2.getAddress().getPort());

    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    when(helixResourceManager.getServerToSegmentsMap(TABLE_NAME)).thenReturn(serverToSegments);
    when(helixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet())).thenReturn(endpoints);

    TableMetadataReader reader = new TableMetadataReader(_executor, _connectionManager, helixResourceManager);
    Map<String, Map<String, String>> result = reader.getSegmentCrcs(TABLE_NAME, null, TIMEOUT_MS);

    assertEquals(result, Map.of(
        "seg_A", Map.of("Server_host0_8098", "111", "Server_host1_8098", "999"),
        "seg_B", Map.of("Server_host0_8098", "222", "Server_host2_8098", "222")));
  }

  /// Filtering by segments only queries servers hosting those segments and omits unrequested segments from the result.
  @Test
  public void testGetSegmentCrcsWithSegmentsFilter()
      throws Exception {
    AtomicInteger s0Hits = new AtomicInteger();
    AtomicInteger s1Hits = new AtomicInteger();
    AtomicInteger s2Hits = new AtomicInteger();
    HttpServer s0 = startCrcServer(TABLE_NAME, 200,
        JsonUtils.objectToString(Map.of("seg_A", "111", "seg_B", "222")), s0Hits);
    HttpServer s1 = startCrcServer(TABLE_NAME, 200,
        JsonUtils.objectToString(Map.of("seg_A", "111")), s1Hits);
    HttpServer s2 = startCrcServer(TABLE_NAME, 200,
        JsonUtils.objectToString(Map.of("seg_B", "333")), s2Hits);

    Map<String, List<String>> serverToSegments = Map.of(
        "Server_host0_8098", List.of("seg_A", "seg_B"),
        "Server_host1_8098", List.of("seg_A"),
        "Server_host2_8098", List.of("seg_B"));
    Set<String> expectedServers = Set.of("Server_host0_8098", "Server_host2_8098");
    BiMap<String, String> endpoints = HashBiMap.create();
    endpoints.put("Server_host0_8098", "http://localhost:" + s0.getAddress().getPort());
    endpoints.put("Server_host2_8098", "http://localhost:" + s2.getAddress().getPort());

    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    when(helixResourceManager.getServerToSegmentsMap(TABLE_NAME)).thenReturn(serverToSegments);
    when(helixResourceManager.getDataInstanceAdminEndpoints(expectedServers)).thenReturn(endpoints);

    TableMetadataReader reader = new TableMetadataReader(_executor, _connectionManager, helixResourceManager);
    Map<String, Map<String, String>> result = reader.getSegmentCrcs(TABLE_NAME, List.of("seg_B"), TIMEOUT_MS);

    assertEquals(result, Map.of("seg_B", Map.of("Server_host0_8098", "222", "Server_host2_8098", "333")));
    assertEquals(s0Hits.get(), 1);
    assertEquals(s1Hits.get(), 0);
    assertEquals(s2Hits.get(), 1);
    verify(helixResourceManager).getDataInstanceAdminEndpoints(expectedServers);
  }

  /// Empty tables and segment filters matching no servers return an empty map without contacting servers.
  @Test
  public void testGetSegmentCrcsEmptyTableAndUnmatchedFilter()
      throws Exception {
    PinotHelixResourceManager emptyHelix = mock(PinotHelixResourceManager.class);
    when(emptyHelix.getServerToSegmentsMap(TABLE_NAME)).thenReturn(Map.of());
    TableMetadataReader emptyReader = new TableMetadataReader(_executor, _connectionManager, emptyHelix);

    assertEquals(emptyReader.getSegmentCrcs(TABLE_NAME, null, TIMEOUT_MS), Map.of());
    verify(emptyHelix, never()).getDataInstanceAdminEndpoints(anySet());

    PinotHelixResourceManager helix = mock(PinotHelixResourceManager.class);
    when(helix.getServerToSegmentsMap(TABLE_NAME)).thenReturn(Map.of("Server_host0_8098", List.of("seg_A")));
    TableMetadataReader reader = new TableMetadataReader(_executor, _connectionManager, helix);

    assertEquals(reader.getSegmentCrcs(TABLE_NAME, List.of("seg_missing"), TIMEOUT_MS), Map.of());
    verify(helix, never()).getDataInstanceAdminEndpoints(anySet());
  }

  /// A healthy server that has not yet loaded an assigned segment is simply absent for that segment.
  @Test
  public void testGetSegmentCrcsMissingSegmentOnOneReplica()
      throws Exception {
    HttpServer s0 = startCrcServer(TABLE_NAME, 200, JsonUtils.objectToString(Map.of("seg_A", "111")), null);
    HttpServer s1 = startCrcServer(TABLE_NAME, 200, "{}", null);

    Map<String, List<String>> serverToSegments = Map.of(
        "Server_host0_8098", List.of("seg_A"),
        "Server_host1_8098", List.of("seg_A"));
    BiMap<String, String> endpoints = HashBiMap.create();
    endpoints.put("Server_host0_8098", "http://localhost:" + s0.getAddress().getPort());
    endpoints.put("Server_host1_8098", "http://localhost:" + s1.getAddress().getPort());

    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    when(helixResourceManager.getServerToSegmentsMap(TABLE_NAME)).thenReturn(serverToSegments);
    when(helixResourceManager.getDataInstanceAdminEndpoints(anySet())).thenReturn(endpoints);

    TableMetadataReader reader = new TableMetadataReader(_executor, _connectionManager, helixResourceManager);
    Map<String, Map<String, String>> result = reader.getSegmentCrcs(TABLE_NAME, null, TIMEOUT_MS);

    assertEquals(result, Map.of("seg_A", Map.of("Server_host0_8098", "111")));
  }

  /// Any failing server (500, 404, or malformed JSON) must fail the whole call instead of returning partial CRCs.
  @Test
  public void testGetSegmentCrcsFailsOnServerError()
      throws Exception {
    for (Object[] failureCase : new Object[][]{{500, ""}, {404, ""}, {200, "not-json"}, {200, "null"}}) {
      int statusCode = (int) failureCase[0];
      String body = (String) failureCase[1];
      HttpServer healthy = startCrcServer(TABLE_NAME, 200, JsonUtils.objectToString(Map.of("seg_A", "111")), null);
      HttpServer broken = startCrcServer(TABLE_NAME, statusCode, body, null);

      Map<String, List<String>> serverToSegments = Map.of(
          "Server_host0_8098", List.of("seg_A"),
          "Server_host1_8098", List.of("seg_A"));
      BiMap<String, String> endpoints = HashBiMap.create();
      endpoints.put("Server_host0_8098", "http://localhost:" + healthy.getAddress().getPort());
      endpoints.put("Server_host1_8098", "http://localhost:" + broken.getAddress().getPort());

      PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.getServerToSegmentsMap(TABLE_NAME)).thenReturn(serverToSegments);
      when(helixResourceManager.getDataInstanceAdminEndpoints(anySet())).thenReturn(endpoints);

      TableMetadataReader reader = new TableMetadataReader(_executor, _connectionManager, helixResourceManager);
      assertThrows(IOException.class, () -> reader.getSegmentCrcs(TABLE_NAME, null, TIMEOUT_MS));
    }
  }

  /// Database-prefixed table names are properly encoded when requesting server CRC endpoints.
  @Test
  public void testGetSegmentCrcsDatabasePrefixedTable()
      throws Exception {
    String dbTableName = "myDb.testTable_OFFLINE";
    HttpServer server = startCrcServer(dbTableName, 200, JsonUtils.objectToString(Map.of("seg_A", "555")), null);

    Map<String, List<String>> serverToSegments = Map.of("Server_host0_8098", List.of("seg_A"));
    BiMap<String, String> endpoints = HashBiMap.create();
    endpoints.put("Server_host0_8098", "http://localhost:" + server.getAddress().getPort());

    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    when(helixResourceManager.getServerToSegmentsMap(dbTableName)).thenReturn(serverToSegments);
    when(helixResourceManager.getDataInstanceAdminEndpoints(anySet())).thenReturn(endpoints);

    TableMetadataReader reader = new TableMetadataReader(_executor, _connectionManager, helixResourceManager);
    Map<String, Map<String, String>> result = reader.getSegmentCrcs(dbTableName, null, TIMEOUT_MS);

    assertEquals(result, Map.of("seg_A", Map.of("Server_host0_8098", "555")));
  }

  private HttpServer startCrcServer(String tableNameWithType, int statusCode, String responseBody,
      @Nullable AtomicInteger hitCounter)
      throws IOException {
    byte[] bodyBytes = responseBody.getBytes(StandardCharsets.UTF_8);
    HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    httpServer.createContext("/tables/" + tableNameWithType + "/segments/crc", exchange -> {
      if (hitCounter != null) {
        hitCounter.incrementAndGet();
      }
      if (statusCode >= 300) {
        exchange.sendResponseHeaders(statusCode, -1);
        exchange.close();
        return;
      }
      exchange.sendResponseHeaders(statusCode, bodyBytes.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(bodyBytes);
      }
    });
    httpServer.start();
    _httpServers.add(httpServer);
    return httpServer;
  }

  private static Set<String> segmentNames(JsonNode metadata) {
    Set<String> segmentNames = new HashSet<>();
    metadata.fieldNames().forEachRemaining(segmentNames::add);
    return segmentNames;
  }
}

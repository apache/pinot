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
package org.apache.pinot.controller.api.resources;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class PinotSegmentRestletResourceTest {

  @Mock
  PinotHelixResourceManager _pinotHelixResourceManager;

  @InjectMocks
  PinotSegmentRestletResource _pinotSegmentRestletResource;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testGetPartitionIdToSegmentsToDeleteMap() {
    IdealState idealState = mock(IdealState.class);
    ZNRecord znRecord = mock(ZNRecord.class);
    String tableName = "testTable";
    long currentTime = System.currentTimeMillis();
    Map<String, Map<String, String>> segmentsToInstanceState = new HashMap<>();

    // Add segments for partition 0
    for (String segment : getSegmentForPartition(tableName, 0, 0, 10, currentTime)) {
      segmentsToInstanceState.put(segment, null);
    }

    // Add segments for partition 1
    for (String segment : getSegmentForPartition(tableName, 1, 0, 10, currentTime)) {
      segmentsToInstanceState.put(segment, null);
    }

    // Add segments for partition 2. None of the segments of this partition should be deleted.
    for (String segment : getSegmentForPartition(tableName, 2, 0, 10, currentTime)) {
      segmentsToInstanceState.put(segment, null);
    }

    // Mock response for fetching segment to instance state map
    when(idealState.getRecord()).thenReturn(znRecord);
    when(znRecord.getMapFields()).thenReturn(segmentsToInstanceState);

    // Create the partition to oldest segment map
    Map<Integer, LLCSegmentName> partitionToOldestSegment = Map.of(
        0, new LLCSegmentName(tableName, 0, 3, currentTime),
        1, new LLCSegmentName(tableName, 1, 5, currentTime)
    );

    // This map will be populated by the method
    Map<Integer, LLCSegmentName> partitionIdToLatestSegment = new HashMap<>();

    // Create the expected response map
    Map<Integer, Set<String>> expectedResponse = new HashMap<>();
    expectedResponse.put(0,
        getSegmentForPartition(tableName, 0, 3, 7, currentTime).stream().collect(Collectors.toSet()));
    expectedResponse.put(1,
        getSegmentForPartition(tableName, 1, 5, 5, currentTime).stream().collect(Collectors.toSet()));

    // Call the method and check the result
    Map<Integer, Set<String>> result = _pinotSegmentRestletResource.getPartitionIdToSegmentsToDeleteMap(
        partitionToOldestSegment, segmentsToInstanceState.keySet(), partitionIdToLatestSegment);

    assertEquals(expectedResponse, result);

    // Verify that partitionIdToLatestSegment has been populated with the latest segment for each partition
    assertEquals(2, partitionIdToLatestSegment.size());
    assertEquals(9, partitionIdToLatestSegment.get(0).getSequenceNumber());
    assertEquals(9, partitionIdToLatestSegment.get(1).getSequenceNumber());
  }

  @Test
  public void testGetPartitionIDToOldestSegment() {
    List<String> segments = new ArrayList<>();
    String tableName = "testTable";
    long currentTime = System.currentTimeMillis();

    // Add segments for testing
    segments.addAll(getSegmentForPartition(tableName, 0, 3, 3, currentTime)); // Segments with seq 3,4,5 for partition 0
    segments.addAll(getSegmentForPartition(tableName, 1, 4, 2, currentTime)); // Segments with seq 4,5 for partition 1

    // Only add the above segment to the ideal state segment list
    Set<String> idealStateSegmentSet = new HashSet<>(segments);

    // Add a segment from another table to this list that has lower sequence ID for the above partitions
    segments.addAll(
        getSegmentForPartition(tableName + "fake", 0, 1, 3, currentTime)); // Segments with seq 1,2,3 for partition 0

    // Create expected result map
    Map<Integer, LLCSegmentName> expectedResult = new HashMap<>();
    expectedResult.put(0, new LLCSegmentName(tableName, 0, 3, currentTime));
    expectedResult.put(1, new LLCSegmentName(tableName, 1, 4, currentTime));

    // Call the method and check the result
    Map<Integer, LLCSegmentName> result =
        _pinotSegmentRestletResource.getPartitionIDToOldestSegment(segments, idealStateSegmentSet);

    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetSegmentsWithInvalidPartitionMetadata()
      throws TableNotFoundException {
    String tableName = "testTable";
    String tableNameWithType = "testTable_OFFLINE";
    when(_pinotHelixResourceManager.getExistingTableNamesWithType(eq(tableName), any()))
        .thenReturn(List.of(tableNameWithType));

    // seg0: null partition metadata, no column filter → valid
    SegmentZKMetadata seg0 = new SegmentZKMetadata("seg0");

    // seg1: single partition per column, no column filter → valid
    SegmentZKMetadata seg1 = new SegmentZKMetadata("seg1");
    seg1.getSimpleFields().put(CommonConstants.Segment.PARTITION_METADATA,
        "{\"columnPartitionMap\":{\"col\":{\"functionName\":\"Modulo\",\"numPartitions\":4,\"partitions\":[0]}}}");

    // seg2: multiple partitions for a column, no column filter → invalid
    SegmentZKMetadata seg2 = new SegmentZKMetadata("seg2");
    seg2.getSimpleFields().put(CommonConstants.Segment.PARTITION_METADATA,
        "{\"columnPartitionMap\":{\"col\":{\"functionName\":\"Modulo\",\"numPartitions\":4,\"partitions\":[0,1]}}}");

    // seg3: malformed JSON → invalid
    SegmentZKMetadata seg3 = new SegmentZKMetadata("seg3");
    seg3.getSimpleFields().put(CommonConstants.Segment.PARTITION_METADATA, "not-valid-json");

    when(_pinotHelixResourceManager.getSegmentsZKMetadata(tableNameWithType))
        .thenReturn(List.of(seg0, seg1, seg2, seg3));

    // no column filter: seg0 (null) is valid, seg1 is valid, seg2 (multi-partition) and seg3 (malformed) are invalid
    Map<String, String> result =
        _pinotSegmentRestletResource.getSegmentsWithInvalidPartitionMetadata(tableName, "OFFLINE", null, null);
    assertEquals(result.size(), 2);
    assertTrue(result.containsKey("seg2"));
    assertTrue(result.containsKey("seg3"));
  }

  @Test
  public void testGetSegmentsWithInvalidPartitionMetadataWithColumnFilter()
      throws TableNotFoundException {
    String tableName = "testTable2";
    String tableNameWithType = "testTable2_OFFLINE";
    when(_pinotHelixResourceManager.getExistingTableNamesWithType(eq(tableName), any()))
        .thenReturn(List.of(tableNameWithType));

    // seg0: null partition metadata, column specified → invalid
    SegmentZKMetadata seg0 = new SegmentZKMetadata("seg0");

    // seg1: single partition for the specified column → valid
    SegmentZKMetadata seg1 = new SegmentZKMetadata("seg1");
    seg1.getSimpleFields().put(CommonConstants.Segment.PARTITION_METADATA,
        "{\"columnPartitionMap\":{\"col\":{\"functionName\":\"Modulo\",\"numPartitions\":4,\"partitions\":[2]}}}");

    // seg2: multiple partitions for the specified column → invalid
    SegmentZKMetadata seg2 = new SegmentZKMetadata("seg2");
    seg2.getSimpleFields().put(CommonConstants.Segment.PARTITION_METADATA,
        "{\"columnPartitionMap\":{\"col\":{\"functionName\":\"Modulo\",\"numPartitions\":4,\"partitions\":[0,1]}}}");

    // seg3: specified column not present in partition metadata → invalid
    SegmentZKMetadata seg3 = new SegmentZKMetadata("seg3");
    seg3.getSimpleFields().put(CommonConstants.Segment.PARTITION_METADATA,
        "{\"columnPartitionMap\":{\"otherCol\":{\"functionName\":\"Modulo\",\"numPartitions\":4,\"partitions\":[0]}}}");

    when(_pinotHelixResourceManager.getSegmentsZKMetadata(tableNameWithType))
        .thenReturn(List.of(seg0, seg1, seg2, seg3));

    // with column filter "col": seg0 (null metadata), seg2 (multi-partition), seg3 (column absent) are invalid
    Map<String, String> result =
        _pinotSegmentRestletResource.getSegmentsWithInvalidPartitionMetadata(tableName, "OFFLINE", "col", null);
    assertEquals(result.size(), 3);
    assertTrue(result.containsKey("seg0"));
    assertTrue(result.containsKey("seg2"));
    assertTrue(result.containsKey("seg3"));
  }

  /// 1 segment x 3 replicas without columns is 3 entries. Every case is rejected before any server is contacted.
  @Test
  public void testGetServerMetadataEntryLimits()
      throws Exception {
    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setProperty(ControllerConf.SEGMENT_METADATA_DEFAULT_ENTRY_LIMIT, 2);
    controllerConf.setProperty(ControllerConf.SEGMENT_METADATA_MAX_ENTRY_LIMIT, 10);
    _pinotSegmentRestletResource._controllerConf = controllerConf;
    when(_pinotHelixResourceManager.getExistingTableNamesWithType(eq("testTable"), any()))
        .thenReturn(List.of("testTable_OFFLINE"));
    when(_pinotHelixResourceManager.getServerToSegmentsMap("testTable_OFFLINE"))
        .thenReturn(Map.of("server0", List.of("seg0"), "server1", List.of("seg0"), "server2", List.of("seg0")));

    // Omitted: the configured default applies
    assertServerMetadataRejected(null, "above the limit of 2");
    // Explicit: overrides the default, so a smaller value applies too
    assertServerMetadataRejected(1, "above the limit of 1");
    // Explicit: must be positive and at most the configured max
    assertServerMetadataRejected(0, ControllerConf.SEGMENT_METADATA_MAX_ENTRY_LIMIT);
    assertServerMetadataRejected(11, ControllerConf.SEGMENT_METADATA_MAX_ENTRY_LIMIT);
    verify(_pinotHelixResourceManager, never()).getDataInstanceAdminEndpoints(anySet());
  }

  /// 1 segment x 3 replicas without columns is 3 entries. An explicit maxEntries above the configured default and
  /// equal to the configured max is accepted, so the max is inclusive.
  @Test
  public void testGetServerMetadataAtMaxEntryLimit()
      throws Exception {
    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setProperty(ControllerConf.SEGMENT_METADATA_DEFAULT_ENTRY_LIMIT, 2);
    controllerConf.setProperty(ControllerConf.SEGMENT_METADATA_MAX_ENTRY_LIMIT, 3);
    _pinotSegmentRestletResource._controllerConf = controllerConf;
    ExecutorService executor = Executors.newFixedThreadPool(3);
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    _pinotSegmentRestletResource._executor = executor;
    _pinotSegmentRestletResource._connectionManager = connectionManager;
    List<HttpServer> httpServers = new ArrayList<>();
    try {
      BiMap<String, String> serverToEndpoint = HashBiMap.create();
      for (String server : List.of("server0", "server1", "server2")) {
        HttpServer httpServer = startServer("seg0");
        httpServers.add(httpServer);
        serverToEndpoint.put(server, "http://localhost:" + httpServer.getAddress().getPort());
      }
      when(_pinotHelixResourceManager.getExistingTableNamesWithType(eq("testTable"), any()))
          .thenReturn(List.of("testTable_OFFLINE"));
      when(_pinotHelixResourceManager.getServerToSegmentsMap("testTable_OFFLINE"))
          .thenReturn(Map.of("server0", List.of("seg0"), "server1", List.of("seg0"), "server2", List.of("seg0")));
      when(_pinotHelixResourceManager.getDataInstanceAdminEndpoints(anySet())).thenReturn(serverToEndpoint);

      String metadata = _pinotSegmentRestletResource.getServerMetadata("testTable", "OFFLINE", List.of(), List.of(),
          3, mock(HttpHeaders.class));

      assertEquals(JsonUtils.stringToJsonNode(metadata).get("seg0").get("segmentName").asText(), "seg0");
    } finally {
      httpServers.forEach(server -> server.stop(0));
      executor.shutdownNow();
      connectionManager.close();
    }
  }

  /// Starts a fake server whose table-level metadata endpoint returns metadata for the given segment.
  private static HttpServer startServer(String segment)
      throws IOException {
    byte[] body = JsonUtils.objectToString(
            JsonUtils.newObjectNode().set(segment, JsonUtils.newObjectNode().put("segmentName", segment)))
        .getBytes(StandardCharsets.UTF_8);
    HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    httpServer.createContext("/tables/testTable_OFFLINE/segments/metadata", exchange -> {
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    });
    httpServer.start();
    return httpServer;
  }

  private void assertServerMetadataRejected(@Nullable Integer maxEntries, String expectedMessage) {
    ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
        () -> _pinotSegmentRestletResource.getServerMetadata("testTable", "OFFLINE", List.of(), List.of(), maxEntries,
            mock(HttpHeaders.class)));
    assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
  }

  @Test
  public void testGetServerSegmentCrcsValidation() {
    HttpHeaders headers = mock(HttpHeaders.class);

    // Missing table type suffix -> 400 BAD_REQUEST
    ControllerApplicationException missingType = expectThrows(ControllerApplicationException.class,
        () -> _pinotSegmentRestletResource.getServerSegmentCrcs("testTable", List.of(), headers));
    assertEquals(missingType.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

    // Invalid table name with multiple dots -> 400 BAD_REQUEST
    ControllerApplicationException invalidDots = expectThrows(ControllerApplicationException.class,
        () -> _pinotSegmentRestletResource.getServerSegmentCrcs("a.b.testTable_OFFLINE", List.of(), headers));
    assertEquals(invalidDots.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

    // Conflicting database prefix and header -> 400 BAD_REQUEST
    HttpHeaders conflictingHeaders = mock(HttpHeaders.class);
    when(conflictingHeaders.getHeaderString(CommonConstants.DATABASE)).thenReturn("otherDb");
    ControllerApplicationException dbConflict = expectThrows(ControllerApplicationException.class,
        () -> _pinotSegmentRestletResource.getServerSegmentCrcs("myDb.testTable_OFFLINE", List.of(),
            conflictingHeaders));
    assertEquals(dbConflict.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

    // Blank segment filter value -> 400 BAD_REQUEST
    ControllerApplicationException emptySegment = expectThrows(ControllerApplicationException.class,
        () -> _pinotSegmentRestletResource.getServerSegmentCrcs("testTable_OFFLINE", List.of(""), headers));
    assertEquals(emptySegment.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    ControllerApplicationException blankSegment = expectThrows(ControllerApplicationException.class,
        () -> _pinotSegmentRestletResource.getServerSegmentCrcs("testTable_OFFLINE", List.of("seg_A", " "), headers));
    assertEquals(blankSegment.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

    // Non-existent table -> 404 NOT_FOUND
    when(_pinotHelixResourceManager.hasTable("missingTable_OFFLINE")).thenReturn(false);
    ControllerApplicationException missingTable = expectThrows(ControllerApplicationException.class,
        () -> _pinotSegmentRestletResource.getServerSegmentCrcs("missingTable_OFFLINE", List.of(), headers));
    assertEquals(missingTable.getResponse().getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void testGetServerSegmentCrcsSuccessAndDatabaseHeader()
      throws Exception {
    ControllerConf prevConf = _pinotSegmentRestletResource._controllerConf;
    Executor prevExecutor = _pinotSegmentRestletResource._executor;
    HttpClientConnectionManager prevConnMgr = _pinotSegmentRestletResource._connectionManager;
    _pinotSegmentRestletResource._controllerConf = new ControllerConf();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    _pinotSegmentRestletResource._executor = executor;
    _pinotSegmentRestletResource._connectionManager = connectionManager;
    List<HttpServer> httpServers = new ArrayList<>();
    try {
      String translatedTable = "myDb.testTable_OFFLINE";
      HttpServer s0 = startCrcServer(translatedTable, 200,
          JsonUtils.objectToString(Map.of("seg_A", "111", "seg_B", "222")));
      HttpServer s1 = startCrcServer(translatedTable, 200,
          JsonUtils.objectToString(Map.of("seg_A", "111")));
      httpServers.add(s0);
      httpServers.add(s1);

      BiMap<String, String> serverToEndpoint = HashBiMap.create();
      serverToEndpoint.put("Server_host0_8098", "http://localhost:" + s0.getAddress().getPort());
      serverToEndpoint.put("Server_host1_8098", "http://localhost:" + s1.getAddress().getPort());

      HttpHeaders headers = mock(HttpHeaders.class);
      when(headers.getHeaderString(CommonConstants.DATABASE)).thenReturn("myDb");
      when(_pinotHelixResourceManager.hasTable(translatedTable)).thenReturn(true);
      when(_pinotHelixResourceManager.getServerToSegmentsMap(translatedTable)).thenReturn(Map.of(
          "Server_host0_8098", List.of("seg_A", "seg_B"),
          "Server_host1_8098", List.of("seg_A")));
      when(_pinotHelixResourceManager.getDataInstanceAdminEndpoints(anySet())).thenReturn(serverToEndpoint);

      Map<String, Map<String, String>> allCrcs =
          _pinotSegmentRestletResource.getServerSegmentCrcs("testTable_OFFLINE", List.of(), headers);
      assertEquals(allCrcs, Map.of(
          "seg_A", Map.of("Server_host0_8098", "111", "Server_host1_8098", "111"),
          "seg_B", Map.of("Server_host0_8098", "222")));

      BiMap<String, String> s0OnlyEndpoint = HashBiMap.create();
      s0OnlyEndpoint.put("Server_host0_8098", "http://localhost:" + s0.getAddress().getPort());
      when(_pinotHelixResourceManager.getDataInstanceAdminEndpoints(Set.of("Server_host0_8098")))
          .thenReturn(s0OnlyEndpoint);

      Map<String, Map<String, String>> filteredCrcs =
          _pinotSegmentRestletResource.getServerSegmentCrcs("testTable_OFFLINE", List.of("seg_B"), headers);
      assertEquals(filteredCrcs, Map.of("seg_B", Map.of("Server_host0_8098", "222")));
    } finally {
      _pinotSegmentRestletResource._controllerConf = prevConf;
      _pinotSegmentRestletResource._executor = prevExecutor;
      _pinotSegmentRestletResource._connectionManager = prevConnMgr;
      httpServers.forEach(server -> server.stop(0));
      executor.shutdownNow();
      connectionManager.close();
    }
  }

  @Test
  public void testGetServerSegmentCrcsErrorMapping()
      throws Exception {
    ControllerConf prevConf = _pinotSegmentRestletResource._controllerConf;
    Executor prevExecutor = _pinotSegmentRestletResource._executor;
    HttpClientConnectionManager prevConnMgr = _pinotSegmentRestletResource._connectionManager;
    _pinotSegmentRestletResource._controllerConf = new ControllerConf();
    ExecutorService executor = Executors.newFixedThreadPool(1);
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    _pinotSegmentRestletResource._executor = executor;
    _pinotSegmentRestletResource._connectionManager = connectionManager;
    HttpServer failingServer = null;
    try {
      String tableName = "testTable_OFFLINE";
      HttpHeaders headers = mock(HttpHeaders.class);
      when(_pinotHelixResourceManager.hasTable(tableName)).thenReturn(true);

      // TOCTOU missing IdealState -> 404 NOT_FOUND
      when(_pinotHelixResourceManager.getServerToSegmentsMap(tableName))
          .thenThrow(new IllegalStateException("Ideal state does not exist for table: " + tableName));
      ControllerApplicationException missingIdealState = expectThrows(ControllerApplicationException.class,
          () -> _pinotSegmentRestletResource.getServerSegmentCrcs(tableName, List.of(), headers));
      assertEquals(missingIdealState.getResponse().getStatus(), Response.Status.NOT_FOUND.getStatusCode());

      // InvalidConfigException (missing Helix InstanceConfig) -> 500 INTERNAL_SERVER_ERROR
      doReturn(Map.of("Server_host0_8098", List.of("seg_A")))
          .when(_pinotHelixResourceManager).getServerToSegmentsMap(tableName);
      when(_pinotHelixResourceManager.getDataInstanceAdminEndpoints(anySet()))
          .thenThrow(new InvalidConfigException("Missing endpoint config"));
      ControllerApplicationException badConfig = expectThrows(ControllerApplicationException.class,
          () -> _pinotSegmentRestletResource.getServerSegmentCrcs(tableName, List.of(), headers));
      assertEquals(badConfig.getResponse().getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());

      // Server HTTP 500 -> 500 INTERNAL_SERVER_ERROR naming the failed server
      failingServer = startCrcServer(tableName, 500, "");
      BiMap<String, String> endpoints = HashBiMap.create();
      endpoints.put("Server_host0_8098", "http://localhost:" + failingServer.getAddress().getPort());
      doReturn(endpoints).when(_pinotHelixResourceManager).getDataInstanceAdminEndpoints(anySet());
      ControllerApplicationException serverErr = expectThrows(ControllerApplicationException.class,
          () -> _pinotSegmentRestletResource.getServerSegmentCrcs(tableName, List.of(), headers));
      assertEquals(serverErr.getResponse().getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
      assertTrue(serverErr.getMessage().contains("Server_host0_8098"), serverErr.getMessage());
    } finally {
      _pinotSegmentRestletResource._controllerConf = prevConf;
      _pinotSegmentRestletResource._executor = prevExecutor;
      _pinotSegmentRestletResource._connectionManager = prevConnMgr;
      if (failingServer != null) {
        failingServer.stop(0);
      }
      executor.shutdownNow();
      connectionManager.close();
    }
  }

  private static HttpServer startCrcServer(String tableNameWithType, int statusCode, String responseBody)
      throws IOException {
    byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
    HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    httpServer.createContext("/tables/" + tableNameWithType + "/segments/crc", exchange -> {
      if (statusCode >= 300) {
        exchange.sendResponseHeaders(statusCode, -1);
        exchange.close();
        return;
      }
      exchange.sendResponseHeaders(statusCode, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    });
    httpServer.start();
    return httpServer;
  }

  private List<String> getSegmentForPartition(String tableName, int partitionID, int sequenceNumberOffset,
      int numberOfSegments, long currentTime) {
    List<String> segments = new ArrayList<>();
    for (int i = sequenceNumberOffset; i < sequenceNumberOffset + numberOfSegments; i++) {
      segments.add(new LLCSegmentName(tableName, partitionID, i, currentTime).getSegmentName());
    }
    return segments;
  }
}

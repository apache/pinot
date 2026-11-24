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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
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

  private List<String> getSegmentForPartition(String tableName, int partitionID, int sequenceNumberOffset,
      int numberOfSegments, long currentTime) {
    List<String> segments = new ArrayList<>();
    for (int i = sequenceNumberOffset; i < sequenceNumberOffset + numberOfSegments; i++) {
      segments.add(new LLCSegmentName(tableName, partitionID, i, currentTime).getSegmentName());
    }
    return segments;
  }
}

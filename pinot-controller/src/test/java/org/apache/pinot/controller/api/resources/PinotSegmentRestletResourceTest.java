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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


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
  public void testGetServerToSegments() {
    String tableName = "testTable";
    Map<String, List<String>> fullServerToSegmentsMap = new HashMap<>();
    fullServerToSegmentsMap.put("svr01", new ArrayList<>(List.of("seg01", "seg02")));
    fullServerToSegmentsMap.put("svr02", new ArrayList<>(List.of("seg02", "seg03")));
    fullServerToSegmentsMap.put("svr03", new ArrayList<>(List.of("seg03", "seg01")));
    when(_pinotHelixResourceManager.getServerToSegmentsMap(tableName, null)).thenReturn(fullServerToSegmentsMap);
    when(_pinotHelixResourceManager.getServerToSegmentsMap(tableName, "svr02")).thenReturn(
        Map.of("svr02", new ArrayList<>(List.of("seg02", "seg03"))));
    when(_pinotHelixResourceManager.getServers(tableName, "seg01")).thenReturn(Set.of("svr01", "svr03"));

    // Get all servers and all their segments.
    Map<String, List<String>> serverToSegmentsMap =
        _pinotSegmentRestletResource.getServerToSegments(tableName, null, null);
    assertEquals(serverToSegmentsMap, fullServerToSegmentsMap);

    // Get all segments on svr02.
    serverToSegmentsMap = _pinotSegmentRestletResource.getServerToSegments(tableName, null, "svr02");
    assertEquals(serverToSegmentsMap, Map.of("svr02", List.of("seg02", "seg03")));

    // Get all servers with seg01.
    serverToSegmentsMap = _pinotSegmentRestletResource.getServerToSegments(tableName, "seg01", null);
    assertEquals(serverToSegmentsMap, Map.of("svr01", List.of("seg01"), "svr03", List.of("seg01")));

    // Simply map the provided server to the provided segments.
    serverToSegmentsMap = _pinotSegmentRestletResource.getServerToSegments(tableName, "seg01", "svr01");
    assertEquals(serverToSegmentsMap, Map.of("svr01", List.of("seg01")));
    serverToSegmentsMap = _pinotSegmentRestletResource.getServerToSegments(tableName, "anySegment", "anyServer");
    assertEquals(serverToSegmentsMap, Map.of("anyServer", List.of("anySegment")));
    serverToSegmentsMap = _pinotSegmentRestletResource.getServerToSegments(tableName, "seg01|seg02", "svr02");
    assertEquals(serverToSegmentsMap, Map.of("svr02", List.of("seg01", "seg02")));
    try {
      _pinotSegmentRestletResource.getServerToSegments(tableName, "seg01,seg02", null);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Only one segment is expected but got: [seg01, seg02]"));
    }
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

  private List<String> getSegmentForPartition(String tableName, int partitionID, int sequenceNumberOffset,
      int numberOfSegments, long currentTime) {
    List<String> segments = new ArrayList<>();
    for (int i = sequenceNumberOffset; i < sequenceNumberOffset + numberOfSegments; i++) {
      segments.add(new LLCSegmentName(tableName, partitionID, i, currentTime).getSegmentName());
    }
    return segments;
  }
}

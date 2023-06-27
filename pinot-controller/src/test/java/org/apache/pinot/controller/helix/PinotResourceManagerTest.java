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
package org.apache.pinot.controller.helix;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PinotResourceManagerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final int NUM_REPLICAS = 2;
  private static final String PARTITION_COLUMN = "partitionColumn";

  private final ControllerTest _testInstance = ControllerTest.getInstance();
  private PinotHelixResourceManager _resourceManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    _testInstance.setupSharedStateAndValidate();
    _resourceManager = _testInstance.getHelixResourceManager();

    // Adding an offline table
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    _resourceManager.addTable(offlineTableConfig);

    // Adding an upsert enabled realtime table which consumes from a stream with 2 partitions
    Schema dummySchema = ControllerTest.createDummySchema(RAW_TABLE_NAME);
    _testInstance.addSchema(dummySchema);
    Map<String, String> streamConfigs = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();
    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS)
            .setStreamConfigs(streamConfigs)
            .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(PARTITION_COLUMN, 1))
            .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL)).build();
    _resourceManager.addTable(realtimeTableConfig);
  }

  @Test
  public void testTableCleanupAfterRealtimeClusterException()
      throws Exception {
    String invalidRawTableName = "invalidTable";
    Schema dummySchema = ControllerTest.createDummySchema(invalidRawTableName);
    _testInstance.addSchema(dummySchema);

    // Missing stream config
    TableConfig invalidRealtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(invalidRawTableName).build();
    try {
      _resourceManager.addTable(invalidRealtimeTableConfig);
      fail("Table creation should have thrown exception due to missing stream config in validation config");
    } catch (Exception e) {
      // expected
    }

    // Verify invalid table config is cleaned up
    assertNull(_resourceManager.getTableConfig(invalidRealtimeTableConfig.getTableName()));
  }

  @Test
  public void testUpdateSegmentZKMetadata() {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata("testSegment");

    // Segment ZK metadata does not exist
    assertFalse(_resourceManager.updateZkMetadata(OFFLINE_TABLE_NAME, segmentZKMetadata, 0));

    // Set segment ZK metadata
    assertTrue(_resourceManager.updateZkMetadata(OFFLINE_TABLE_NAME, segmentZKMetadata));

    // Update ZK metadata
    ZNRecord segmentMetadataZnRecord = _resourceManager.getSegmentMetadataZnRecord(OFFLINE_TABLE_NAME, "testSegment");
    assertNotNull(segmentMetadataZnRecord);
    assertEquals(segmentMetadataZnRecord.getVersion(), 0);
    assertTrue(_resourceManager.updateZkMetadata(OFFLINE_TABLE_NAME, segmentZKMetadata, 0));
    segmentMetadataZnRecord = _resourceManager.getSegmentMetadataZnRecord(OFFLINE_TABLE_NAME, "testSegment");
    assertNotNull(segmentMetadataZnRecord);
    assertEquals(segmentMetadataZnRecord.getVersion(), 1);
    assertFalse(_resourceManager.updateZkMetadata(OFFLINE_TABLE_NAME, segmentZKMetadata, 0));
  }

  /**
   * First tests basic segment adding/deleting.
   * Then creates 3 threads that concurrently try to add 10 segments each, and asserts that we have 30 segments in the
   * end. Then launches 3 threads again that concurrently try to delete all segments, and makes sure that we have 0
   * segments left in the end.
   */
  @Test
  public void testBasicAndConcurrentAddingAndDeletingSegments()
      throws Exception {
    PinotHelixResourceManager resourceManager = _resourceManager;

    // Basic add/delete
    for (int i = 1; i <= 2; i++) {
      resourceManager.addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME), "downloadUrl");
    }
    IdealState idealState = resourceManager.getTableIdealState(OFFLINE_TABLE_NAME);
    assertNotNull(idealState);
    Set<String> segments = idealState.getPartitionSet();
    assertEquals(segments.size(), 2);

    for (String segmentName : segments) {
      resourceManager.deleteSegment(OFFLINE_TABLE_NAME, segmentName);
    }
    idealState = resourceManager.getTableIdealState(OFFLINE_TABLE_NAME);
    assertNotNull(idealState);
    assertEquals(idealState.getNumPartitions(), 0);

    // Concurrent add/deletion
    ExecutorService executor = Executors.newFixedThreadPool(3);
    Future<?>[] futures = new Future[3];
    for (int i = 0; i < 3; i++) {
      futures[i] = executor.submit(() -> {
        for (int i1 = 0; i1 < 10; i1++) {
          resourceManager.addNewSegment(OFFLINE_TABLE_NAME,
              SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME), "downloadUrl");
        }
      });
    }
    for (int i = 0; i < 3; i++) {
      futures[i].get();
    }
    idealState = resourceManager.getTableIdealState(OFFLINE_TABLE_NAME);
    assertNotNull(idealState);
    segments = idealState.getPartitionSet();
    assertEquals(segments.size(), 30);

    futures = new Future[30];
    int index = 0;
    for (String segment : segments) {
      futures[index++] = executor.submit(() -> resourceManager.deleteSegment(OFFLINE_TABLE_NAME, segment));
    }
    for (int i = 0; i < 30; i++) {
      futures[i].get();
    }
    idealState = resourceManager.getTableIdealState(OFFLINE_TABLE_NAME);
    assertNotNull(idealState);
    assertEquals(idealState.getNumPartitions(), 0);

    executor.shutdown();
  }

  @Test
  public void testAddingRealtimeTableSegmentsWithPartitionIdInZkMetadata() {
    // Add three segments: two from partition 0 and 1 from partition 1;
    String partition0Segment0 = "p0s0";
    String partition0Segment1 = "p0s1";
    String partition1Segment0 = "p1s0";
    _resourceManager.addNewSegment(REALTIME_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadataWithPartitionInfo(RAW_TABLE_NAME, partition0Segment0,
            PARTITION_COLUMN, 0), "downloadUrl");
    _resourceManager.addNewSegment(REALTIME_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadataWithPartitionInfo(RAW_TABLE_NAME, partition0Segment1,
            PARTITION_COLUMN, 0), "downloadUrl");
    _resourceManager.addNewSegment(REALTIME_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadataWithPartitionInfo(RAW_TABLE_NAME, partition1Segment0,
            PARTITION_COLUMN, 1), "downloadUrl");

    IdealState idealState = _resourceManager.getTableIdealState(REALTIME_TABLE_NAME);
    assertNotNull(idealState);
    Set<String> segments = idealState.getPartitionSet();
    // 2 consuming segments, 3 uploaded segments
    assertEquals(segments.size(), 5);
    assertTrue(segments.contains(partition0Segment0));
    assertTrue(segments.contains(partition0Segment1));
    assertTrue(segments.contains(partition1Segment0));

    // Check the segments of the same partition is assigned to the same set of servers.
    Map<Integer, Set<String>> partitionIdToServersMap = new HashMap<>();
    for (String segment : segments) {
      int partitionId;
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segment);
      if (llcSegmentName != null) {
        partitionId = llcSegmentName.getPartitionGroupId();
      } else {
        partitionId = Integer.parseInt(segment.substring(1, 2));
      }
      Set<String> instances = idealState.getInstanceSet(segment);
      if (partitionIdToServersMap.containsKey(partitionId)) {
        assertEquals(instances, partitionIdToServersMap.get(partitionId));
      } else {
        partitionIdToServersMap.put(partitionId, instances);
      }
    }
  }

  @AfterClass
  public void tearDown() {
    _testInstance.cleanup();
  }
}

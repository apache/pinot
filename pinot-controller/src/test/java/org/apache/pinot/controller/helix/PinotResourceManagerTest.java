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
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.map.HashedMap;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotResourceManagerTest {
  private static final String OFFLINE_TABLE_NAME = "offlineResourceManagerTestTable";
  private static final String REALTIME_TABLE_NAME = "realtimeResourceManagerTestTable";
  private static final String NUM_REPLICAS_STRING = "2";

  @BeforeClass
  public void setUp() throws Exception {
    ControllerTestUtils.setupClusterAndValidate();

    // Adding an offline table
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME).build();
    ControllerTestUtils.getHelixResourceManager().addTable(offlineTableConfig);

    // Adding a realtime table
    Schema dummySchema = ControllerTestUtils.createDummySchema(REALTIME_TABLE_NAME);
    ControllerTestUtils.addSchema(dummySchema);
    Map<String, String> streamConfigs = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();
    TableConfig realtimeTableConfig = new TableConfigBuilder(TableType.REALTIME).setStreamConfigs(streamConfigs).
        setTableName(REALTIME_TABLE_NAME).setSchemaName(dummySchema.getSchemaName()).build();
    realtimeTableConfig.getValidationConfig().setReplicasPerPartition(NUM_REPLICAS_STRING);
    ControllerTestUtils.getHelixResourceManager().addTable(realtimeTableConfig);
  }

  @Test
  public void testUpdateSegmentZKMetadata() {
    OfflineSegmentZKMetadata segmentZKMetadata = new OfflineSegmentZKMetadata();
    segmentZKMetadata.setSegmentName("testSegment");

    // Segment ZK metadata does not exist
    Assert.assertFalse(
        ControllerTestUtils.getHelixResourceManager().updateZkMetadata(OFFLINE_TABLE_NAME + "_OFFLINE", segmentZKMetadata, 0));

    // Set segment ZK metadata
    Assert.assertTrue(
        ControllerTestUtils.getHelixResourceManager().updateZkMetadata(OFFLINE_TABLE_NAME + "_OFFLINE", segmentZKMetadata));

    // Update ZK metadata
    Assert.assertEquals(
        ControllerTestUtils.getHelixResourceManager().getSegmentMetadataZnRecord(OFFLINE_TABLE_NAME + "_OFFLINE", "testSegment").getVersion(), 0);
    Assert.assertTrue(
        ControllerTestUtils.getHelixResourceManager().updateZkMetadata(OFFLINE_TABLE_NAME + "_OFFLINE", segmentZKMetadata, 0));
    Assert.assertEquals(
        ControllerTestUtils.getHelixResourceManager().getSegmentMetadataZnRecord(OFFLINE_TABLE_NAME + "_OFFLINE", "testSegment").getVersion(), 1);
    Assert.assertFalse(
        ControllerTestUtils.getHelixResourceManager().updateZkMetadata(OFFLINE_TABLE_NAME + "_OFFLINE", segmentZKMetadata, 0));
  }

  /**
   * First tests basic segment adding/deleting.
   * Then creates 3 threads that concurrently try to add 10 segments each, and asserts that we have
   * 100 segments in the end. Then launches 5 threads again that concurrently try to delete all segments,
   * and makes sure that we have zero segments left in the end.
   * @throws Exception
   */

  @Test
  public void testBasicAndConcurrentAddingAndDeletingSegments() throws Exception {
    final String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(OFFLINE_TABLE_NAME);

    // Basic add/delete case
    for (int i = 1; i <= 2; i++) {
      ControllerTestUtils.getHelixResourceManager().addNewSegment(
          OFFLINE_TABLE_NAME, SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME),
          "downloadUrl");
    }
    IdealState idealState = ControllerTestUtils
        .getHelixAdmin().getResourceIdealState(ControllerTestUtils.getHelixClusterName(), offlineTableName);
    Set<String> segments = idealState.getPartitionSet();
    Assert.assertEquals(segments.size(), 2);

    for (String segmentName : segments) {
      ControllerTestUtils.getHelixResourceManager().deleteSegment(offlineTableName, segmentName);
    }
    idealState = ControllerTestUtils
        .getHelixAdmin().getResourceIdealState(ControllerTestUtils.getHelixClusterName(), offlineTableName);
    Assert.assertEquals(idealState.getPartitionSet().size(), 0);

    // Concurrent segment deletion
    ExecutorService addSegmentExecutor = Executors.newFixedThreadPool(3);
    for (int i = 0; i < 3; ++i) {
      addSegmentExecutor.execute(new Runnable() {
        @Override
        public void run() {
          for (int i = 0; i < 10; i++) {
            ControllerTestUtils.getHelixResourceManager().addNewSegment(OFFLINE_TABLE_NAME,
                SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME), "downloadUrl");
          }
        }
      });
    }
    addSegmentExecutor.shutdown();
    addSegmentExecutor.awaitTermination(1, TimeUnit.MINUTES);

    idealState = ControllerTestUtils
        .getHelixAdmin().getResourceIdealState(ControllerTestUtils.getHelixClusterName(), offlineTableName);
    Assert.assertEquals(idealState.getPartitionSet().size(), 30);

    ExecutorService deleteSegmentExecutor = Executors.newFixedThreadPool(3);
    for (final String segmentName : idealState.getPartitionSet()) {
      deleteSegmentExecutor.execute(new Runnable() {
        @Override
        public void run() {
          ControllerTestUtils.getHelixResourceManager().deleteSegment(offlineTableName, segmentName);
        }
      });
    }
    deleteSegmentExecutor.shutdown();
    deleteSegmentExecutor.awaitTermination(1, TimeUnit.MINUTES);

    idealState = ControllerTestUtils
        .getHelixAdmin().getResourceIdealState(ControllerTestUtils.getHelixClusterName(), offlineTableName);
    Assert.assertEquals(idealState.getPartitionSet().size(), 0);
  }

  @Test
  public void testAddingRealtimeTableSegments() {
    // Basic add/delete case
    String segmentName =  "realtimeResourceManagerTestTable__1__0__onetimeunique";
    ControllerTestUtils.getHelixResourceManager().addNewSegment(
          REALTIME_TABLE_NAME, SegmentMetadataMockUtils.mockSegmentMetadata(REALTIME_TABLE_NAME, segmentName),
          "downloadUrl");

    IdealState idealState = ControllerTestUtils
        .getHelixAdmin().getResourceIdealState(ControllerTestUtils.getHelixClusterName(), TableNameBuilder.REALTIME.tableNameWithType(REALTIME_TABLE_NAME));
    Set<String> segments = idealState.getPartitionSet();
    Assert.assertEquals(segments.size(), 3);
    Assert.assertTrue(segments.contains(segmentName));
    // Check the segments of the same partition is assigned to the same set of servers.
    Map<Integer, Set<String>> segmentAssignment = new HashMap<>();
    for (String segment : segments) {
      Assert.assertTrue(LLCSegmentName.isLowLevelConsumerSegmentName(segment));
      Integer partitionId = new LLCSegmentName(segment).getPartitionId();
      Set<String> instances = idealState.getInstanceSet(segment);
      if (segmentAssignment.containsKey(partitionId)) {
        Assert.assertEquals(instances, segmentAssignment.get(partitionId));
      } else {
        segmentAssignment.put(partitionId, instances);
      }
    }
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }
}

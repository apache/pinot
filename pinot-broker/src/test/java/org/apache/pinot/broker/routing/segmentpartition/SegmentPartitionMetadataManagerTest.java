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
package org.apache.pinot.broker.routing.segmentpartition;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class SegmentPartitionMetadataManagerTest extends ControllerTest {
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static final String PARTITION_COLUMN = "memberId";
  private static final String PARTITION_COLUMN_FUNC = "Murmur";
  private static final int NUM_PARTITIONS = 2;
  private static final String PARTITION_COLUMN_FUNC_ALT = "Modulo";
  private static final int NUM_PARTITIONS_ALT = 4;
  private static final String SERVER_0 = "server0";
  private static final String SERVER_1 = "server1";

  private ZkClient _zkClient;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @BeforeClass
  public void setUp() {
    startZk();
    _zkClient = new ZkClient(getZkUrl(), ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT,
        new ZNRecordSerializer());
    _propertyStore =
        new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(_zkClient), "/TimeBoundaryManagerTest/PROPERTYSTORE", null);
  }

  @AfterClass
  public void tearDown() {
    _zkClient.close();
    stopZk();
  }

  @Test
  public void testPartitionMetadataManagerProcessingThroughSegmentChangesSinglePartitionTable() {
    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
    Map<String, String> onlineInstanceStateMap = ImmutableMap.of(SERVER_0, ONLINE, SERVER_1, ONLINE);
    Set<String> onlineSegments = new HashSet<>();
    // NOTE: Ideal state is not used in the current implementation.
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);

    SegmentPartitionMetadataManager partitionMetadataManager =
        new SegmentPartitionMetadataManager(OFFLINE_TABLE_NAME, PARTITION_COLUMN, PARTITION_COLUMN_FUNC,
            NUM_PARTITIONS);
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(OFFLINE_TABLE_NAME, _propertyStore);
    segmentZkMetadataFetcher.register(partitionMetadataManager);

    // Initial state should be all empty
    segmentZkMetadataFetcher.init(idealState, externalView, onlineSegments);
    TablePartitionInfo tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    assertEquals(tablePartitionInfo.getPartitionInfoMap(), new TablePartitionInfo.PartitionInfo[NUM_PARTITIONS]);
    assertTrue(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Adding segment without partition metadata should be recorded in the invalid segments
    String segmentWithoutPartitionMetadata = "segmentWithoutPartitionMetadata";
    onlineSegments.add(segmentWithoutPartitionMetadata);
    segmentAssignment.put(segmentWithoutPartitionMetadata, onlineInstanceStateMap);
    SegmentZKMetadata segmentZKMetadataWithoutPartitionMetadata =
        new SegmentZKMetadata(segmentWithoutPartitionMetadata);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME,
        segmentZKMetadataWithoutPartitionMetadata);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    assertEquals(tablePartitionInfo.getPartitionInfoMap(), new TablePartitionInfo.PartitionInfo[NUM_PARTITIONS]);
    assertEquals(tablePartitionInfo.getSegmentsWithInvalidPartition(),
        Collections.singletonList(segmentWithoutPartitionMetadata));

    // Removing segment without partition metadata should remove it from the invalid segments
    onlineSegments.remove(segmentWithoutPartitionMetadata);
    segmentAssignment.remove(segmentWithoutPartitionMetadata);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    assertEquals(tablePartitionInfo.getPartitionInfoMap(), new TablePartitionInfo.PartitionInfo[NUM_PARTITIONS]);
    assertTrue(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Same logic applies to the new segment
    onlineSegments.add(segmentWithoutPartitionMetadata);
    segmentAssignment.put(segmentWithoutPartitionMetadata, onlineInstanceStateMap);
    segmentZKMetadataWithoutPartitionMetadata = new SegmentZKMetadata(segmentWithoutPartitionMetadata);
    segmentZKMetadataWithoutPartitionMetadata.setPushTime(System.currentTimeMillis());
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME,
        segmentZKMetadataWithoutPartitionMetadata);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    assertEquals(tablePartitionInfo.getPartitionInfoMap(), new TablePartitionInfo.PartitionInfo[NUM_PARTITIONS]);
    assertEquals(tablePartitionInfo.getSegmentsWithInvalidPartition(),
        Collections.singletonList(segmentWithoutPartitionMetadata));
    onlineSegments.remove(segmentWithoutPartitionMetadata);
    segmentAssignment.remove(segmentWithoutPartitionMetadata);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    assertEquals(tablePartitionInfo.getPartitionInfoMap(), new TablePartitionInfo.PartitionInfo[NUM_PARTITIONS]);
    assertTrue(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Adding segments inline with the partition column config should yield correct partition results
    String segment0 = "segment0";
    onlineSegments.add(segment0);
    segmentAssignment.put(segment0, Collections.singletonMap(SERVER_0, ONLINE));
    setSegmentZKMetadata(segment0, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 0, 0L);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    TablePartitionInfo.PartitionInfo[] partitionInfoMap = tablePartitionInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Collections.singleton(segment0));
    assertNull(partitionInfoMap[1]);
    assertTrue(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Adding one more segments
    String segment1 = "segment1";
    onlineSegments.add(segment1);
    segmentAssignment.put(segment1, Collections.singletonMap(SERVER_1, ONLINE));
    setSegmentZKMetadata(segment1, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 1, 0L);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    partitionInfoMap = tablePartitionInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Collections.singleton(segment0));
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Collections.singleton(SERVER_1));
    assertEquals(partitionInfoMap[1]._segments, Collections.singleton(segment1));
    assertTrue(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Updating partition metadata without refreshing should have no effect
    setSegmentZKMetadata(segment0, PARTITION_COLUMN_FUNC_ALT, NUM_PARTITIONS_ALT, 0, 0L);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    partitionInfoMap = tablePartitionInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Collections.singleton(segment0));
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Collections.singleton(SERVER_1));
    assertEquals(partitionInfoMap[1]._segments, Collections.singleton(segment1));
    assertTrue(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Refreshing the changed segment should update the partition info
    segmentZkMetadataFetcher.refreshSegment(segment0);
    // segment0 is no longer inline with the table config, and it should be recorded in the invalid segments
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    partitionInfoMap = tablePartitionInfo.getPartitionInfoMap();
    assertNull(partitionInfoMap[0]);
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Collections.singleton(SERVER_1));
    assertEquals(partitionInfoMap[1]._segments, Collections.singleton(segment1));
    assertEquals(tablePartitionInfo.getSegmentsWithInvalidPartition(), Collections.singletonList(segment0));

    // Refresh the changed segment back to inline, and both segments should now be back on the partition list
    setSegmentZKMetadata(segment0, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 0, 0L);
    segmentZkMetadataFetcher.refreshSegment(segment0);
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    partitionInfoMap = tablePartitionInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Collections.singleton(segment0));
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Collections.singleton(SERVER_1));
    assertEquals(partitionInfoMap[1]._segments, Collections.singleton(segment1));
    assertTrue(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Changing one of the segments to be on a different server should update the fully replicated servers
    segmentAssignment.put(segment1, Collections.singletonMap(SERVER_0, ONLINE));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    partitionInfoMap = tablePartitionInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Collections.singleton(segment0));
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEquals(partitionInfoMap[1]._segments, Collections.singleton(segment1));
    assertTrue(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Adding one more segment to partition-1 but located on a different server will update the partition map, but
    // remove the fully replicated server because it is no longer having full replica on a single server
    String segment2 = "segment2";
    onlineSegments.add(segment2);
    segmentAssignment.put(segment2, Collections.singletonMap(SERVER_1, ONLINE));
    setSegmentZKMetadata(segment2, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 1, 0L);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    partitionInfoMap = tablePartitionInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Collections.singleton(segment0));
    assertTrue(partitionInfoMap[1]._fullyReplicatedServers.isEmpty());
    assertEqualsNoOrder(partitionInfoMap[1]._segments.toArray(), new String[]{segment1, segment2});
    assertTrue(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Updating the segment to be replicated on 2 servers should add the fully replicated server back
    segmentAssignment.put(segment2, ImmutableMap.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    partitionInfoMap = tablePartitionInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Collections.singleton(segment0));
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEqualsNoOrder(partitionInfoMap[1]._segments.toArray(), new String[]{segment1, segment2});
    assertTrue(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Adding a newly created segment without available replica should not update the partition map
    String newSegment = "newSegment";
    onlineSegments.add(newSegment);
    setSegmentZKMetadata(newSegment, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 0, System.currentTimeMillis());
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    partitionInfoMap = tablePartitionInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Collections.singleton(segment0));
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEqualsNoOrder(partitionInfoMap[1]._segments.toArray(), new String[]{segment1, segment2});
    assertTrue(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Making all of them replicated will show full list, even for the new segment
    segmentAssignment.put(segment0, ImmutableMap.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentAssignment.put(segment1, ImmutableMap.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentAssignment.put(segment2, ImmutableMap.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentAssignment.put(newSegment, ImmutableMap.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionInfo = partitionMetadataManager.getTablePartitionInfo();
    partitionInfoMap = tablePartitionInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, ImmutableSet.of(SERVER_0, SERVER_1));
    assertEqualsNoOrder(partitionInfoMap[0]._segments.toArray(), new String[]{segment0, newSegment});
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, ImmutableSet.of(SERVER_0, SERVER_1));
    assertEqualsNoOrder(partitionInfoMap[1]._segments.toArray(), new String[]{segment1, segment2});
    assertTrue(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty());
  }

  private void setSegmentZKMetadata(String segment, String partitionFunction, int numPartitions, int partitionId,
      long creationTimeMs) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segment);
    segmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(Collections.singletonMap(PARTITION_COLUMN,
        new ColumnPartitionMetadata(partitionFunction, numPartitions, Collections.singleton(partitionId), null))));
    segmentZKMetadata.setCreationTime(creationTimeMs);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME, segmentZKMetadata);
  }
}

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

import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ERROR;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.testng.Assert.*;


public class SegmentPartitionMetadataManagerTest extends ControllerTest {
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static final String PARTITION_COLUMN = "memberId";
  private static final String PARTITION_COLUMN_FUNC = "Murmur";
  private static final int NUM_PARTITIONS = 2;
  private static final String PARTITION_COLUMN_FUNC_ALT = "Modulo";
  private static final int NUM_PARTITIONS_ALT = 4;
  private static final String SERVER_0 = "server0";
  private static final String SERVER_1 = "server1";
  private static final String SERVER_2 = "server2";

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
    Map<String, String> onlineInstanceStateMap = Map.of(SERVER_0, ONLINE, SERVER_1, ONLINE);
    Set<String> onlineSegments = new HashSet<>();
    // NOTE: The empty ideal state intentionally exercises the time based fallback path of the pending segment check,
    //       where the target servers cannot be determined from the ideal state.
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);

    SegmentPartitionMetadataManager partitionMetadataManager =
        new SegmentPartitionMetadataManager(OFFLINE_TABLE_NAME, PARTITION_COLUMN, PARTITION_COLUMN_FUNC, NUM_PARTITIONS,
            TimeUnit.MINUTES.toMillis(5));
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(OFFLINE_TABLE_NAME, _propertyStore);
    segmentZkMetadataFetcher.register(partitionMetadataManager);

    // Initial state should be all empty
    segmentZkMetadataFetcher.init(idealState, externalView, onlineSegments);
    TablePartitionReplicatedServersInfo tablePartitionReplicatedServersInfo =
        partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    assertEquals(tablePartitionReplicatedServersInfo.getPartitionInfoMap(),
        new TablePartitionReplicatedServersInfo.PartitionInfo[NUM_PARTITIONS]);
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Adding segment without partition metadata should be recorded in the invalid segments
    String segmentWithoutPartitionMetadata = "segmentWithoutPartitionMetadata";
    onlineSegments.add(segmentWithoutPartitionMetadata);
    segmentAssignment.put(segmentWithoutPartitionMetadata, onlineInstanceStateMap);
    SegmentZKMetadata segmentZKMetadataWithoutPartitionMetadata =
        new SegmentZKMetadata(segmentWithoutPartitionMetadata);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME,
        segmentZKMetadataWithoutPartitionMetadata);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    assertEquals(tablePartitionReplicatedServersInfo.getPartitionInfoMap(),
        new TablePartitionReplicatedServersInfo.PartitionInfo[NUM_PARTITIONS]);
    assertEquals(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition(),
        List.of(segmentWithoutPartitionMetadata));

    // Removing segment without partition metadata should remove it from the invalid segments
    onlineSegments.remove(segmentWithoutPartitionMetadata);
    segmentAssignment.remove(segmentWithoutPartitionMetadata);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    assertEquals(tablePartitionReplicatedServersInfo.getPartitionInfoMap(),
        new TablePartitionReplicatedServersInfo.PartitionInfo[NUM_PARTITIONS]);
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Same logic applies to the new segment
    onlineSegments.add(segmentWithoutPartitionMetadata);
    segmentAssignment.put(segmentWithoutPartitionMetadata, onlineInstanceStateMap);
    segmentZKMetadataWithoutPartitionMetadata = new SegmentZKMetadata(segmentWithoutPartitionMetadata);
    segmentZKMetadataWithoutPartitionMetadata.setPushTime(System.currentTimeMillis());
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME,
        segmentZKMetadataWithoutPartitionMetadata);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    assertEquals(tablePartitionReplicatedServersInfo.getPartitionInfoMap(),
        new TablePartitionReplicatedServersInfo.PartitionInfo[NUM_PARTITIONS]);
    assertEquals(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition(),
        List.of(segmentWithoutPartitionMetadata));
    onlineSegments.remove(segmentWithoutPartitionMetadata);
    segmentAssignment.remove(segmentWithoutPartitionMetadata);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    assertEquals(tablePartitionReplicatedServersInfo.getPartitionInfoMap(),
        new TablePartitionReplicatedServersInfo.PartitionInfo[NUM_PARTITIONS]);
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Adding segments inline with the partition column config should yield correct partition results
    String segment0 = "segment0";
    onlineSegments.add(segment0);
    segmentAssignment.put(segment0, Map.of(SERVER_0, ONLINE));
    setSegmentZKMetadata(segment0, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 0, 0L);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    TablePartitionReplicatedServersInfo.PartitionInfo[] partitionInfoMap =
        tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Set.of(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Set.of(segment0));
    assertNull(partitionInfoMap[1]);
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Adding one more segments
    String segment1 = "segment1";
    onlineSegments.add(segment1);
    segmentAssignment.put(segment1, Map.of(SERVER_1, ONLINE));
    setSegmentZKMetadata(segment1, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 1, 0L);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Set.of(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Set.of(segment0));
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Set.of(SERVER_1));
    assertEquals(partitionInfoMap[1]._segments, Set.of(segment1));
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Updating partition metadata without refreshing should have no effect
    setSegmentZKMetadata(segment0, PARTITION_COLUMN_FUNC_ALT, NUM_PARTITIONS_ALT, 0, 0L);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Set.of(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Set.of(segment0));
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Set.of(SERVER_1));
    assertEquals(partitionInfoMap[1]._segments, Set.of(segment1));
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Refreshing the changed segment should update the partition info
    segmentZkMetadataFetcher.refreshSegment(segment0);
    // segment0 is no longer inline with the table config, and it should be recorded in the invalid segments
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertNull(partitionInfoMap[0]);
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Set.of(SERVER_1));
    assertEquals(partitionInfoMap[1]._segments, Set.of(segment1));
    assertEquals(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition(),
        List.of(segment0));

    // Refresh the changed segment back to inline, and both segments should now be back on the partition list
    setSegmentZKMetadata(segment0, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 0, 0L);
    segmentZkMetadataFetcher.refreshSegment(segment0);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Set.of(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Set.of(segment0));
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Set.of(SERVER_1));
    assertEquals(partitionInfoMap[1]._segments, Set.of(segment1));
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Changing one of the segments to be on a different server should update the fully replicated servers
    segmentAssignment.put(segment1, Map.of(SERVER_0, ONLINE));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Set.of(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Set.of(segment0));
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Set.of(SERVER_0));
    assertEquals(partitionInfoMap[1]._segments, Set.of(segment1));
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Adding one more segment to partition-1 but located on a different server will update the partition map, but
    // remove the fully replicated server because it is no longer having full replica on a single server
    String segment2 = "segment2";
    onlineSegments.add(segment2);
    segmentAssignment.put(segment2, Map.of(SERVER_1, ONLINE));
    setSegmentZKMetadata(segment2, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 1, 0L);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Set.of(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Set.of(segment0));
    assertTrue(partitionInfoMap[1]._fullyReplicatedServers.isEmpty());
    assertEqualsNoOrder(partitionInfoMap[1]._segments.toArray(), new String[]{segment1, segment2});
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Updating the segment to be replicated on 2 servers should add the fully replicated server back
    segmentAssignment.put(segment2, Map.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Set.of(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Set.of(segment0));
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Set.of(SERVER_0));
    assertEqualsNoOrder(partitionInfoMap[1]._segments.toArray(), new String[]{segment1, segment2});
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Adding a newly created segment without available replica should not update the partition map
    String newSegment = "newSegment";
    onlineSegments.add(newSegment);
    setSegmentZKMetadata(newSegment, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 0, System.currentTimeMillis());
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Set.of(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Set.of(segment0));
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Set.of(SERVER_0));
    assertEqualsNoOrder(partitionInfoMap[1]._segments.toArray(), new String[]{segment1, segment2});
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Making all of them replicated will show full list, even for the new segment
    segmentAssignment.put(segment0, Map.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentAssignment.put(segment1, Map.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentAssignment.put(segment2, Map.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentAssignment.put(newSegment, Map.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, ImmutableSet.of(SERVER_0, SERVER_1));
    assertEqualsNoOrder(partitionInfoMap[0]._segments.toArray(), new String[]{segment0, newSegment});
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, ImmutableSet.of(SERVER_0, SERVER_1));
    assertEqualsNoOrder(partitionInfoMap[1]._segments.toArray(), new String[]{segment1, segment2});
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Adding one more segment - which has an invalid partition ID greater than NUM_PARTITIONS
    String segmentInvalid = "segment_invalid";
    onlineSegments.add(segmentInvalid);
    segmentAssignment.put(segmentInvalid, Map.of(SERVER_1, ONLINE));
    // partition ID 10000 greater than NUM_PARTITIONS (2).
    setSegmentZKMetadata(segmentInvalid, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 10000, 0L);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, ImmutableSet.of(SERVER_0, SERVER_1));
    assertEqualsNoOrder(partitionInfoMap[0]._segments.toArray(), new String[]{segment0, newSegment});
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, ImmutableSet.of(SERVER_0, SERVER_1));
    assertEqualsNoOrder(partitionInfoMap[1]._segments.toArray(), new String[]{segment1, segment2});
    assertFalse(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());
    assertEquals(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().get(0), segmentInvalid);
  }

  @Test
  public void testReplicationStateTracking() {
    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, String> onlineInstanceStateMap = Map.of(SERVER_0, ONLINE, SERVER_1, ONLINE);
    Set<String> onlineSegments = new HashSet<>();

    SegmentPartitionMetadataManager partitionMetadataManager =
        new SegmentPartitionMetadataManager(OFFLINE_TABLE_NAME, PARTITION_COLUMN, PARTITION_COLUMN_FUNC, NUM_PARTITIONS,
            TimeUnit.MINUTES.toMillis(5));
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(OFFLINE_TABLE_NAME, _propertyStore);
    segmentZkMetadataFetcher.register(partitionMetadataManager);
    segmentZkMetadataFetcher.init(idealState, externalView, onlineSegments);

    // segmentA is fully replicated on both servers
    String segmentA = "trackingSegmentA";
    onlineSegments.add(segmentA);
    idealStateAssignment.put(segmentA, onlineInstanceStateMap);
    externalViewAssignment.put(segmentA, onlineInstanceStateMap);
    setSegmentZKMetadata(segmentA, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 0, 0L);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    TablePartitionReplicatedServersInfo tablePartitionReplicatedServersInfo =
        partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    TablePartitionReplicatedServersInfo.PartitionInfo[] partitionInfoMap =
        tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, ImmutableSet.of(SERVER_0, SERVER_1));
    assertEquals(partitionInfoMap[0]._segments, Collections.singleton(segmentA));

    // segmentB is old by creation time, but has never been fully replicated per its ideal assignment (only ONLINE on
    // SERVER_1 while assigned to both servers). It should be excluded from the partition info instead of reducing the
    // fully replicated servers, regardless of its creation time.
    String segmentB = "trackingSegmentB";
    onlineSegments.add(segmentB);
    idealStateAssignment.put(segmentB, onlineInstanceStateMap);
    externalViewAssignment.put(segmentB, Map.of(SERVER_1, ONLINE));
    setSegmentZKMetadata(segmentB, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 0, 0L);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, ImmutableSet.of(SERVER_0, SERVER_1));
    assertEquals(partitionInfoMap[0]._segments, Collections.singleton(segmentA));
    assertTrue(tablePartitionReplicatedServersInfo.getUnavailableSegments().isEmpty());

    // Once segmentB becomes fully replicated, it should be included as a regular segment
    externalViewAssignment.put(segmentB, onlineInstanceStateMap);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, ImmutableSet.of(SERVER_0, SERVER_1));
    assertEqualsNoOrder(partitionInfoMap[0]._segments.toArray(), new String[]{segmentA, segmentB});

    // Losing a replica after having been fully replicated should reduce the fully replicated servers, so that queries
    // are routed to the remaining replica
    externalViewAssignment.put(segmentB, Map.of(SERVER_0, ONLINE, SERVER_1, ERROR));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEqualsNoOrder(partitionInfoMap[0]._segments.toArray(), new String[]{segmentA, segmentB});

    // Losing all replicas should exclude the segment from the partition info and report it as unavailable, instead of
    // clearing the fully replicated servers and failing all the queries on the partition
    externalViewAssignment.put(segmentB, Map.of(SERVER_0, ERROR, SERVER_1, ERROR));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, ImmutableSet.of(SERVER_0, SERVER_1));
    assertEquals(partitionInfoMap[0]._segments, Collections.singleton(segmentA));
    assertEquals(tablePartitionReplicatedServersInfo.getUnavailableSegments(),
        Collections.singletonList(segmentB));

    // Recovering all replicas should include the segment back as a regular segment
    externalViewAssignment.put(segmentB, onlineInstanceStateMap);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, ImmutableSet.of(SERVER_0, SERVER_1));
    assertEqualsNoOrder(partitionInfoMap[0]._segments.toArray(), new String[]{segmentA, segmentB});
    assertTrue(tablePartitionReplicatedServersInfo.getUnavailableSegments().isEmpty());

    // Changing the ideal assignment (e.g. rebalance) should reset the replication state. While moving towards the new
    // assignment, the segment should not reduce the fully replicated servers, but should still be included when its
    // online servers cover them.
    idealStateAssignment.put(segmentB, Map.of(SERVER_0, ONLINE, SERVER_2, ONLINE));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, ImmutableSet.of(SERVER_0, SERVER_1));
    assertEqualsNoOrder(partitionInfoMap[0]._segments.toArray(), new String[]{segmentA, segmentB});

    // Once the segment converges to the new assignment, it should reduce the fully replicated servers as a regular
    // segment
    externalViewAssignment.put(segmentB, Map.of(SERVER_0, ONLINE, SERVER_2, ONLINE));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEqualsNoOrder(partitionInfoMap[0]._segments.toArray(), new String[]{segmentA, segmentB});
  }

  @Test
  public void testPendingSegmentExpiration()
      throws InterruptedException {
    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, String> onlineInstanceStateMap = Map.of(SERVER_0, ONLINE, SERVER_1, ONLINE);
    Set<String> onlineSegments = new HashSet<>();

    // Use 0 expiration time so that the pending state expires right away
    SegmentPartitionMetadataManager partitionMetadataManager =
        new SegmentPartitionMetadataManager(OFFLINE_TABLE_NAME, PARTITION_COLUMN, PARTITION_COLUMN_FUNC, NUM_PARTITIONS,
            0L);
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(OFFLINE_TABLE_NAME, _propertyStore);
    segmentZkMetadataFetcher.register(partitionMetadataManager);
    segmentZkMetadataFetcher.init(idealState, externalView, onlineSegments);

    // segmentX is fully replicated on both servers; segmentY is assigned to both servers but only ONLINE on SERVER_1
    String segmentX = "expirationSegmentX";
    onlineSegments.add(segmentX);
    idealStateAssignment.put(segmentX, onlineInstanceStateMap);
    externalViewAssignment.put(segmentX, onlineInstanceStateMap);
    setSegmentZKMetadata(segmentX, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 0, 0L);
    String segmentY = "expirationSegmentY";
    onlineSegments.add(segmentY);
    idealStateAssignment.put(segmentY, onlineInstanceStateMap);
    externalViewAssignment.put(segmentY, Map.of(SERVER_1, ONLINE));
    setSegmentZKMetadata(segmentY, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 0, 0L);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);

    // After the pending state expires, segmentY should be treated as a regular segment and reduce the fully
    // replicated servers, so that its data is still served from the available replica
    Thread.sleep(10);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    TablePartitionReplicatedServersInfo tablePartitionReplicatedServersInfo =
        partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    TablePartitionReplicatedServersInfo.PartitionInfo[] partitionInfoMap =
        tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Collections.singleton(SERVER_1));
    assertEqualsNoOrder(partitionInfoMap[0]._segments.toArray(), new String[]{segmentX, segmentY});
  }

  private void setSegmentZKMetadata(String segment, String partitionFunction, int numPartitions, int partitionId,
      long creationTimeMs) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segment);
    segmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(Map.of(PARTITION_COLUMN,
        new ColumnPartitionMetadata(partitionFunction, numPartitions, Set.of(partitionId), null))));
    segmentZKMetadata.setCreationTime(creationTimeMs);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME, segmentZKMetadata);
  }
}

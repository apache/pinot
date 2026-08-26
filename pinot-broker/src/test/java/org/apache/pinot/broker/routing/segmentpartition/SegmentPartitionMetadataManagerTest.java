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
import java.util.HashMap;
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
    // NOTE: Ideal state is not used in the current implementation.
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
    // Partition 0 is still served by segment0, so it is not a deferred empty partition
    assertTrue(tablePartitionReplicatedServersInfo.getPartitionsWithOnlyDeferredSegments().isEmpty());

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

  /// A partition whose only segments are new ones without all replicas available holds data that no single server can
  /// serve as a whole, so it must be told apart from a genuinely empty partition (see
  /// [TablePartitionReplicatedServersInfo#getPartitionsWithOnlyDeferredSegments()]).
  @Test
  public void testPartitionsWithOnlyDeferredSegments() {
    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>();
    // NOTE: Ideal state is not used in the current implementation.
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
    assertTrue(tablePartitionReplicatedServersInfo.getPartitionsWithOnlyDeferredSegments().isEmpty());

    // A newly created segment without available replica as the only segment of partition 1 leaves the partition without
    // partition info, and should be reported as a deferred empty partition. Partition 0 has no segment at all, and
    // should not be reported.
    long creationTimeMs = System.currentTimeMillis();
    String newSegmentWithoutReplica = "deferredSegment1";
    onlineSegments.add(newSegmentWithoutReplica);
    setSegmentZKMetadata(newSegmentWithoutReplica, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 1, creationTimeMs);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    TablePartitionReplicatedServersInfo.PartitionInfo[] partitionInfoMap =
        tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertNull(partitionInfoMap[0]);
    assertNull(partitionInfoMap[1]);
    assertEquals(tablePartitionReplicatedServersInfo.getPartitionsWithOnlyDeferredSegments(), Set.of(1));
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());

    // Adding another newly created segment with all replicas available to partition 1 makes the partition servable. The
    // first segment is still excluded, but the partition is no longer deferred empty. This holds regardless of the
    // order the 2 new segments are processed in, which is why the deferred empty partitions are derived from the final
    // partition info map instead of being latched when a segment is excluded.
    String newSegmentWithReplicas = "deferredSegment2";
    onlineSegments.add(newSegmentWithReplicas);
    segmentAssignment.put(newSegmentWithReplicas, Map.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    setSegmentZKMetadata(newSegmentWithReplicas, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 1, creationTimeMs);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEquals(partitionInfoMap[1]._fullyReplicatedServers, Set.of(SERVER_0, SERVER_1));
    assertEquals(partitionInfoMap[1]._segments, List.of(newSegmentWithReplicas));
    assertTrue(tablePartitionReplicatedServersInfo.getPartitionsWithOnlyDeferredSegments().isEmpty());

    // Bringing up the replicas of the first segment adds it to the partition info
    segmentAssignment.put(newSegmentWithoutReplica, Map.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    tablePartitionReplicatedServersInfo = partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    partitionInfoMap = tablePartitionReplicatedServersInfo.getPartitionInfoMap();
    assertEqualsNoOrder(partitionInfoMap[1]._segments.toArray(),
        new String[]{newSegmentWithoutReplica, newSegmentWithReplicas});
    assertTrue(tablePartitionReplicatedServersInfo.getPartitionsWithOnlyDeferredSegments().isEmpty());
  }

  /// A partition holding both a new segment without any online replica and a new segment with all of them is servable,
  /// so it must never be reported -- whichever of the two the new-segment pass visits first, and one of them IS always
  /// excluded (see the NOTE on the `removeIf` in [SegmentPartitionMetadataManager]).
  ///
  /// The pass walks a [HashMap] keyed by segment name, so the names decide the visit order. This test pins down a name
  /// pair for each of the 2 orders and drives both announcement orders through the manager on top of that.
  @Test
  public void testPartitionsWithOnlyDeferredSegmentsAreOrderIndependent() {
    for (boolean noReplicaVisitedFirst : List.of(true, false)) {
      String[] segmentNames = findSegmentNamePair(noReplicaVisitedFirst);
      for (boolean announceNoReplicaFirst : List.of(true, false)) {
        assertPartitionHasNotOnlyDeferredSegments(segmentNames[0], segmentNames[1], noReplicaVisitedFirst,
            announceNoReplicaFirst);
      }
    }
  }

  /// Returns a `{noReplicaSegment, allReplicasSegment}` name pair that a [HashMap] holding exactly those 2 keys
  /// iterates in the requested order.
  private static String[] findSegmentNamePair(boolean noReplicaVisitedFirst) {
    for (int i = 0; i < 1000; i++) {
      String noReplicaSegment = "deferredNoReplica" + i;
      String allReplicasSegment = "deferredAllReplicas" + i;
      Map<String, String> probe = new HashMap<>();
      probe.put(noReplicaSegment, noReplicaSegment);
      probe.put(allReplicasSegment, allReplicasSegment);
      if (probe.keySet().iterator().next().equals(noReplicaSegment) == noReplicaVisitedFirst) {
        return new String[]{noReplicaSegment, allReplicasSegment};
      }
    }
    throw new AssertionError(
        "Found no segment name pair iterated with the segment " + (noReplicaVisitedFirst ? "without" : "with")
            + " replicas first");
  }

  /// Registers 2 new segments of partition 1 -- one without any online replica and one with all of them -- and asserts
  /// that the partition ends up servable and is NOT reported as deferred. `announceNoReplicaFirst` picks which of the 2
  /// is announced first; `noReplicaVisitedFirst` only feeds the failure message (see [#findSegmentNamePair]).
  private void assertPartitionHasNotOnlyDeferredSegments(String noReplicaSegment, String allReplicasSegment,
      boolean noReplicaVisitedFirst, boolean announceNoReplicaFirst) {
    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>();
    // NOTE: Ideal state is not used in the current implementation.
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);

    SegmentPartitionMetadataManager partitionMetadataManager =
        new SegmentPartitionMetadataManager(OFFLINE_TABLE_NAME, PARTITION_COLUMN, PARTITION_COLUMN_FUNC, NUM_PARTITIONS,
            TimeUnit.MINUTES.toMillis(5));
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(OFFLINE_TABLE_NAME, _propertyStore);
    segmentZkMetadataFetcher.register(partitionMetadataManager);
    segmentZkMetadataFetcher.init(idealState, externalView, onlineSegments);

    long creationTimeMs = System.currentTimeMillis();
    setSegmentZKMetadata(noReplicaSegment, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 1, creationTimeMs);
    setSegmentZKMetadata(allReplicasSegment, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 1, creationTimeMs);
    // Only the second segment has replicas: the first one is absent from the external view altogether.
    segmentAssignment.put(allReplicasSegment, Map.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    onlineSegments.add(announceNoReplicaFirst ? noReplicaSegment : allReplicasSegment);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    onlineSegments.add(announceNoReplicaFirst ? allReplicasSegment : noReplicaSegment);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);

    TablePartitionReplicatedServersInfo tablePartitionReplicatedServersInfo =
        partitionMetadataManager.getTablePartitionReplicatedServersInfo();
    String context = "with the segment without replicas visited " + (noReplicaVisitedFirst ? "first" : "second")
        + " and announced " + (announceNoReplicaFirst ? "first" : "second");
    TablePartitionReplicatedServersInfo.PartitionInfo partitionInfo =
        tablePartitionReplicatedServersInfo.getPartitionInfoMap()[1];
    assertNotNull(partitionInfo, "Partition 1 has no partition info " + context);
    assertEquals(partitionInfo._fullyReplicatedServers, Set.of(SERVER_0, SERVER_1), context);
    assertEquals(partitionInfo._segments, List.of(allReplicasSegment), context);
    assertTrue(tablePartitionReplicatedServersInfo.getPartitionsWithOnlyDeferredSegments().isEmpty(),
        "Servable partition reported as deferred empty " + context + ": "
            + tablePartitionReplicatedServersInfo.getPartitionsWithOnlyDeferredSegments());
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

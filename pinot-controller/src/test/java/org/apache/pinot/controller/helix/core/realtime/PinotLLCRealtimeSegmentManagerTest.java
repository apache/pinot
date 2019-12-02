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
package org.apache.pinot.controller.helix.core.realtime;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsType;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.LLCSegmentCompletionHandlers;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.controller.util.SegmentCompletionUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.core.realtime.stream.OffsetCriteria;
import org.apache.pinot.core.realtime.stream.PartitionLevelStreamConfig;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.filesystem.PinotFSFactory;
import org.apache.zookeeper.data.Stat;
import org.joda.time.Interval;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class PinotLLCRealtimeSegmentManagerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "PinotLLCRealtimeSegmentManagerTest");
  private static final String SCHEME = LLCSegmentCompletionHandlers.getScheme();
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  static final long PARTITION_OFFSET = RANDOM.nextInt(Integer.MAX_VALUE);
  static final long CURRENT_TIME_MS = System.currentTimeMillis();
  static final long START_TIME_MS = CURRENT_TIME_MS - TimeUnit.HOURS.toMillis(RANDOM.nextInt(24) + 24);
  static final long END_TIME_MS = START_TIME_MS + TimeUnit.HOURS.toMillis(RANDOM.nextInt(24) + 1);
  static final Interval INTERVAL = new Interval(START_TIME_MS, END_TIME_MS);
  static final String CRC = Long.toString(RANDOM.nextLong());
  static final String SEGMENT_VERSION =
      RANDOM.nextBoolean() ? SegmentVersion.v1.toString() : SegmentVersion.v3.toString();
  static final int NUM_DOCS = RANDOM.nextInt(Integer.MAX_VALUE) + 1;

  @BeforeClass
  public void setUp() {
    // Printing out the random seed to console so that we can use the seed to reproduce failure conditions
    System.out.println("Using random seed: " + RANDOM_SEED);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  private SegmentMetadataImpl mockSegmentMetadata() {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getTimeInterval()).thenReturn(INTERVAL);
    when(segmentMetadata.getCrc()).thenReturn(CRC);
    when(segmentMetadata.getVersion()).thenReturn(SEGMENT_VERSION);
    when(segmentMetadata.getTotalRawDocs()).thenReturn(NUM_DOCS);
    return segmentMetadata;
  }

  /**
   * Test cases for new table being created, and initial segments setup that follows.
   */
  @Test
  public void testSetUpNewTable() {
    // Insufficient instances - 2 replicas, 1 instance, 4 partitions
    testSetUpNewTable(2, 1, 4, true);

    // Noop path - 2 replicas, 3 instances, 0 partition
    testSetUpNewTable(2, 3, 0, false);

    // Happy paths
    // 2 replicas, 3 instances, 4 partitions
    testSetUpNewTable(2, 3, 4, false);
    // 2 replicas, 3 instances, 8 partitions
    testSetUpNewTable(2, 3, 8, false);
    // 8 replicas, 10 instances, 4 partitions
    testSetUpNewTable(8, 10, 4, false);
  }

  private void testSetUpNewTable(int numReplicas, int numInstances, int numPartitions, boolean expectException) {
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager();
    segmentManager._numReplicas = numReplicas;
    segmentManager.makeTableConfig();
    segmentManager._numInstances = numInstances;
    segmentManager.makeConsumingInstancePartitions();
    segmentManager._numPartitions = numPartitions;

    try {
      segmentManager.setUpNewTable();
      assertFalse(expectException);
    } catch (IllegalStateException e) {
      assertTrue(expectException);
      return;
    }

    Map<String, Map<String, String>> instanceStatesMap = segmentManager._idealState.getRecord().getMapFields();
    assertEquals(instanceStatesMap.size(), numPartitions);
    assertEquals(segmentManager.getAllSegments(REALTIME_TABLE_NAME).size(), numPartitions);

    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(RAW_TABLE_NAME, partitionId, 0, CURRENT_TIME_MS);
      String segmentName = llcSegmentName.getSegmentName();

      Map<String, String> instanceStateMap = instanceStatesMap.get(segmentName);
      assertNotNull(instanceStateMap);
      assertEquals(instanceStateMap.size(), numReplicas);
      for (String state : instanceStateMap.values()) {
        assertEquals(state, RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      }

      LLCRealtimeSegmentZKMetadata segmentZKMetadata =
          segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentName, null);
      assertEquals(segmentZKMetadata.getStatus(), Status.IN_PROGRESS);
      assertEquals(segmentZKMetadata.getStartOffset(), PARTITION_OFFSET);
      assertEquals(segmentZKMetadata.getCreationTime(), CURRENT_TIME_MS);
    }
  }

  private void setUpNewTable(FakePinotLLCRealtimeSegmentManager segmentManager, int numReplicas, int numInstances,
      int numPartitions) {
    segmentManager._numReplicas = numReplicas;
    segmentManager.makeTableConfig();
    segmentManager._numInstances = numInstances;
    segmentManager.makeConsumingInstancePartitions();
    segmentManager._numPartitions = numPartitions;
    segmentManager.setUpNewTable();
  }

  @Test
  public void testCommitSegment() {
    // Set up a new table with 2 replicas, 5 instances, 4 partition
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager();
    setUpNewTable(segmentManager, 2, 5, 4);
    Map<String, Map<String, String>> instanceStatesMap = segmentManager._idealState.getRecord().getMapFields();

    // Commit a segment for partition 0
    String committingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    CommittingSegmentDescriptor committingSegmentDescriptor =
        new CommittingSegmentDescriptor(committingSegment, PARTITION_OFFSET + NUM_DOCS, 0L);
    committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
    segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);

    // Verify instance states for committed segment and new consuming segment
    Map<String, String> committedSegmentInstanceStateMap = instanceStatesMap.get(committingSegment);
    assertNotNull(committedSegmentInstanceStateMap);
    assertEquals(new HashSet<>(committedSegmentInstanceStateMap.values()),
        Collections.singleton(RealtimeSegmentOnlineOfflineStateModel.ONLINE));

    String consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 1, CURRENT_TIME_MS).getSegmentName();
    Map<String, String> consumingSegmentInstanceStateMap = instanceStatesMap.get(consumingSegment);
    assertNotNull(consumingSegmentInstanceStateMap);
    assertEquals(new HashSet<>(consumingSegmentInstanceStateMap.values()),
        Collections.singleton(RealtimeSegmentOnlineOfflineStateModel.CONSUMING));

    // Verify segment ZK metadata for committed segment and new consuming segment
    LLCRealtimeSegmentZKMetadata committedSegmentZKMetadata =
        segmentManager._segmentZKMetadataMap.get(committingSegment);
    assertEquals(committedSegmentZKMetadata.getStatus(), Status.DONE);
    assertEquals(committedSegmentZKMetadata.getStartOffset(), PARTITION_OFFSET);
    assertEquals(committedSegmentZKMetadata.getEndOffset(), PARTITION_OFFSET + NUM_DOCS);
    assertEquals(committedSegmentZKMetadata.getCreationTime(), CURRENT_TIME_MS);
    assertEquals(committedSegmentZKMetadata.getTimeInterval(), INTERVAL);
    assertEquals(committedSegmentZKMetadata.getCrc(), Long.parseLong(CRC));
    assertEquals(committedSegmentZKMetadata.getIndexVersion(), SEGMENT_VERSION);
    assertEquals(committedSegmentZKMetadata.getTotalRawDocs(), NUM_DOCS);

    LLCRealtimeSegmentZKMetadata consumingSegmentZKMetadata =
        segmentManager._segmentZKMetadataMap.get(consumingSegment);
    assertEquals(consumingSegmentZKMetadata.getStatus(), Status.IN_PROGRESS);
    assertEquals(consumingSegmentZKMetadata.getStartOffset(), PARTITION_OFFSET + NUM_DOCS);
    assertEquals(committedSegmentZKMetadata.getCreationTime(), CURRENT_TIME_MS);

    // Turn one instance of the consuming segment OFFLINE and commit the segment
    consumingSegmentInstanceStateMap.entrySet().iterator().next()
        .setValue(RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
    committingSegment = consumingSegment;
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(committingSegment, PARTITION_OFFSET + NUM_DOCS + NUM_DOCS, 0L);
    committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
    segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);

    // Verify instance states for committed segment and new consuming segment
    committedSegmentInstanceStateMap = instanceStatesMap.get(committingSegment);
    assertNotNull(committedSegmentInstanceStateMap);
    assertEquals(new HashSet<>(committedSegmentInstanceStateMap.values()),
        Collections.singleton(RealtimeSegmentOnlineOfflineStateModel.ONLINE));

    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 2, CURRENT_TIME_MS).getSegmentName();
    consumingSegmentInstanceStateMap = instanceStatesMap.get(consumingSegment);
    assertNotNull(consumingSegmentInstanceStateMap);
    assertEquals(new HashSet<>(consumingSegmentInstanceStateMap.values()),
        Collections.singleton(RealtimeSegmentOnlineOfflineStateModel.CONSUMING));

    // Illegal segment commit - commit the segment again
    try {
      segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  /**
   * Test cases for the scenario where stream partitions increase, and the validation manager is attempting to create
   * segments for new partitions. This test assumes that all other factors remain the same (no error conditions or
   * inconsistencies in metadata and ideal state).
   */
  @Test
  public void testSetUpNewPartitions() {
    // Set up a new table with 2 replicas, 5 instances, 0 partition
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager();
    setUpNewTable(segmentManager, 2, 5, 0);

    // No-op
    testSetUpNewPartitions(segmentManager, false);

    // Increase number of partitions from 0 to 2
    segmentManager._numPartitions = 2;
    testSetUpNewPartitions(segmentManager, false);

    // Increase number of partitions form 2 to 4
    segmentManager._numPartitions = 4;
    testSetUpNewPartitions(segmentManager, false);

    // 2 partitions commit segment
    for (int partitionId = 0; partitionId < 2; partitionId++) {
      String segmentName = new LLCSegmentName(RAW_TABLE_NAME, partitionId, 0, CURRENT_TIME_MS).getSegmentName();
      CommittingSegmentDescriptor committingSegmentDescriptor =
          new CommittingSegmentDescriptor(segmentName, PARTITION_OFFSET + NUM_DOCS, 0L);
      committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
      segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
    }
    testSetUpNewPartitions(segmentManager, false);

    // Increase number of partitions form 4 to 6
    segmentManager._numPartitions = 6;
    testSetUpNewPartitions(segmentManager, false);

    // No-op
    testSetUpNewPartitions(segmentManager, false);

    // Reduce number of instances to 1 (illegal because it is less than number of replicas)
    segmentManager._numInstances = 1;
    segmentManager.makeConsumingInstancePartitions();

    // No-op
    testSetUpNewPartitions(segmentManager, false);

    // Increase number of partitions form 6 to 8 (should fail)
    segmentManager._numPartitions = 8;
    testSetUpNewPartitions(segmentManager, true);

    // Should fail again
    testSetUpNewPartitions(segmentManager, true);

    // Increase number of instances back to 5 and allow fixing segments
    segmentManager._numInstances = 5;
    segmentManager.makeConsumingInstancePartitions();
    segmentManager._exceededMaxSegmentCompletionTime = true;

    // Should succeed
    testSetUpNewPartitions(segmentManager, false);
  }

  private void testSetUpNewPartitions(FakePinotLLCRealtimeSegmentManager segmentManager, boolean expectException) {
    Map<String, Map<String, String>> instanceStatesMap = segmentManager._idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> oldInstanceStatesMap = cloneInstanceStatesMap(instanceStatesMap);
    Map<String, LLCRealtimeSegmentZKMetadata> segmentZKMetadataMap = segmentManager._segmentZKMetadataMap;
    Map<String, LLCRealtimeSegmentZKMetadata> oldSegmentZKMetadataMap = cloneSegmentZKMetadataMap(segmentZKMetadataMap);

    try {
      segmentManager.ensureAllPartitionsConsuming();
    } catch (IllegalStateException e) {
      assertTrue(expectException);
      // Restore the old instance states map
      segmentManager._idealState.getRecord().setMapFields(oldInstanceStatesMap);
      return;
    }

    // Check that instance states and ZK metadata remain the same for existing segments
    int oldNumPartitions = 0;
    for (Map.Entry<String, Map<String, String>> entry : oldInstanceStatesMap.entrySet()) {
      String segmentName = entry.getKey();
      assertTrue(instanceStatesMap.containsKey(segmentName));
      assertEquals(instanceStatesMap.get(segmentName), entry.getValue());
      assertTrue(oldSegmentZKMetadataMap.containsKey(segmentName));
      assertTrue(segmentZKMetadataMap.containsKey(segmentName));
      assertEquals(segmentZKMetadataMap.get(segmentName), oldSegmentZKMetadataMap.get(segmentName));
      oldNumPartitions = Math.max(oldNumPartitions, new LLCSegmentName(segmentName).getPartitionId() + 1);
    }

    // Check that for new partitions, each partition should have exactly 1 new segment in CONSUMING state, and metadata
    // in IN_PROGRESS state
    Map<Integer, List<String>> partitionIdToSegmentsMap = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : instanceStatesMap.entrySet()) {
      String segmentName = entry.getKey();
      int partitionId = new LLCSegmentName(segmentName).getPartitionId();
      partitionIdToSegmentsMap.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(segmentName);
    }
    for (int partitionId = oldNumPartitions; partitionId < segmentManager._numPartitions; partitionId++) {
      List<String> segments = partitionIdToSegmentsMap.get(partitionId);
      assertEquals(segments.size(), 1);
      String segmentName = segments.get(0);
      assertFalse(oldInstanceStatesMap.containsKey(segmentName));
      Map<String, String> instanceStateMap = instanceStatesMap.get(segmentName);
      assertEquals(instanceStateMap.size(), segmentManager._numReplicas);
      for (String state : instanceStateMap.values()) {
        assertEquals(state, RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      }
      // NOTE: Old segment ZK metadata might exist when previous round failed due to not enough instances
      assertTrue(segmentZKMetadataMap.containsKey(segmentName));
      LLCRealtimeSegmentZKMetadata segmentZKMetadata = segmentZKMetadataMap.get(segmentName);
      assertEquals(segmentZKMetadata.getStatus(), Status.IN_PROGRESS);
      assertEquals(segmentZKMetadata.getStartOffset(), PARTITION_OFFSET);
      assertEquals(segmentZKMetadata.getCreationTime(), CURRENT_TIME_MS);
    }
  }

  private Map<String, Map<String, String>> cloneInstanceStatesMap(Map<String, Map<String, String>> instanceStatesMap) {
    Map<String, Map<String, String>> clone = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> entry : instanceStatesMap.entrySet()) {
      clone.put(entry.getKey(), new TreeMap<>(entry.getValue()));
    }
    return clone;
  }

  private Map<String, LLCRealtimeSegmentZKMetadata> cloneSegmentZKMetadataMap(
      Map<String, LLCRealtimeSegmentZKMetadata> segmentZKMetadataMap) {
    Map<String, LLCRealtimeSegmentZKMetadata> clone = new HashMap<>();
    for (Map.Entry<String, LLCRealtimeSegmentZKMetadata> entry : segmentZKMetadataMap.entrySet()) {
      clone.put(entry.getKey(), new LLCRealtimeSegmentZKMetadata(entry.getValue().toZNRecord()));
    }
    return clone;
  }

  /**
   * Tests that we can repair all invalid scenarios during segment completion.
   *
   * Segment completion takes place in 3 steps:
   * 1. Update committing segment ZK metadata to status DONE
   * 2. Create new segment ZK metadata with status IN_PROGRESS
   * 3. Update ideal state (change committing segment state to ONLINE and create new segment with state CONSUMING)
   *
   * If a failure happens before step 1 or after step 3, we do not need to fix it.
   * If a failure happens after step 1 is done and before step 3 completes, we will be left in an incorrect state, and
   * should be able to fix it.
   *
   * Scenarios:
   * 1. Step 3 failed - we will find new segment ZK metadata IN_PROGRESS but no segment in ideal state
   * Correction: create new CONSUMING segment in ideal state, update previous CONSUMING segment (if exists) in ideal
   * state to ONLINE
   *
   * 2. Step 2 failed - we will find segment ZK metadata DONE but ideal state CONSUMING
   * Correction: create new segment ZK metadata with state IN_PROGRESS, create new CONSUMING segment in ideal state,
   * update previous CONSUMING segment (if exists) in ideal state to ONLINE
   *
   * 3. All replicas of the new segment are OFFLINE
   * Correction: create new segment ZK metadata with state IN_PROGRESS and consume from the previous start offset,
   * create new CONSUMING segment in ideal state.
   *
   * 4. MaxSegmentCompletionTime: Segment completion has 5 minutes to retry and complete between steps 1 and 3.
   * Correction: Do not correct the segments before the allowed time for segment completion
   */
  @Test
  public void testRepairs() {
    // Set up a new table with 2 replicas, 5 instances, 4 partitions
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager();
    setUpNewTable(segmentManager, 2, 5, 4);
    Map<String, Map<String, String>> instanceStatesMap = segmentManager._idealState.getRecord().getMapFields();

    // Remove the CONSUMING segment from the ideal state for partition 0 (step 3 failed)
    String consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    removeNewConsumingSegment(instanceStatesMap, consumingSegment, null);
    testRepairs(segmentManager);

    // Remove the CONSUMING segment from the ideal state and segment ZK metadata map for partition 0 (step 2 failed)
    removeNewConsumingSegment(instanceStatesMap, consumingSegment, null);
    assertNotNull(segmentManager._segmentZKMetadataMap.remove(consumingSegment));
    testRepairs(segmentManager);

    // 2 partitions commit segment
    for (int partitionId = 0; partitionId < 2; partitionId++) {
      String segmentName = new LLCSegmentName(RAW_TABLE_NAME, partitionId, 0, CURRENT_TIME_MS).getSegmentName();
      CommittingSegmentDescriptor committingSegmentDescriptor =
          new CommittingSegmentDescriptor(segmentName, PARTITION_OFFSET + NUM_DOCS, 0L);
      committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
      segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
    }

    // Remove the CONSUMING segment from the ideal state for partition 0 (step 3 failed)
    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 1, CURRENT_TIME_MS).getSegmentName();
    String latestCommittedSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    removeNewConsumingSegment(instanceStatesMap, consumingSegment, latestCommittedSegment);
    testRepairs(segmentManager);

    // Remove the CONSUMING segment from the ideal state and segment ZK metadata map for partition 0 (step 2 failed)
    removeNewConsumingSegment(instanceStatesMap, consumingSegment, latestCommittedSegment);
    assertNotNull(segmentManager._segmentZKMetadataMap.remove(consumingSegment));
    testRepairs(segmentManager);

    /*
      Test all replicas of the new segment are OFFLINE
     */

    // Set up a new table with 2 replicas, 5 instances, 4 partitions
    segmentManager = new FakePinotLLCRealtimeSegmentManager();
    setUpNewTable(segmentManager, 2, 5, 4);
    instanceStatesMap = segmentManager._idealState.getRecord().getMapFields();

    // Turn all the replicas for the CONSUMING segment to OFFLINE for partition 0
    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    turnNewConsumingSegmentOffline(instanceStatesMap, consumingSegment);
    testRepairs(segmentManager);

    // Turn all the replicas for the CONSUMING segment to OFFLINE for partition 0 again
    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 1, CURRENT_TIME_MS).getSegmentName();
    turnNewConsumingSegmentOffline(instanceStatesMap, consumingSegment);
    testRepairs(segmentManager);

    // 2 partitions commit segment
    for (int partitionId = 0; partitionId < 2; partitionId++) {
      // Sequence number is 2 for partition 0 because segment 0 and 1 are OFFLINE
      int sequenceNumber = partitionId == 0 ? 2 : 0;
      String segmentName =
          new LLCSegmentName(RAW_TABLE_NAME, partitionId, sequenceNumber, CURRENT_TIME_MS).getSegmentName();
      CommittingSegmentDescriptor committingSegmentDescriptor =
          new CommittingSegmentDescriptor(segmentName, PARTITION_OFFSET + NUM_DOCS, 0L);
      committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
      segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
    }

    // Remove the CONSUMING segment from the ideal state for partition 0 (step 3 failed)
    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 3, CURRENT_TIME_MS).getSegmentName();
    latestCommittedSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 2, CURRENT_TIME_MS).getSegmentName();
    removeNewConsumingSegment(instanceStatesMap, consumingSegment, latestCommittedSegment);
    testRepairs(segmentManager);

    // Remove the CONSUMING segment from the ideal state and segment ZK metadata map for partition 0 (step 2 failed)
    removeNewConsumingSegment(instanceStatesMap, consumingSegment, latestCommittedSegment);
    assertNotNull(segmentManager._segmentZKMetadataMap.remove(consumingSegment));
    testRepairs(segmentManager);

    // Turn all the replicas for the CONSUMING segment to OFFLINE for partition 0
    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 3, CURRENT_TIME_MS).getSegmentName();
    turnNewConsumingSegmentOffline(instanceStatesMap, consumingSegment);
    testRepairs(segmentManager);

    // Turn all the replicas for the CONSUMING segment to OFFLINE for partition 0 again
    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 4, CURRENT_TIME_MS).getSegmentName();
    turnNewConsumingSegmentOffline(instanceStatesMap, consumingSegment);
    testRepairs(segmentManager);
  }

  /**
   * Removes the new CONSUMING segment and sets the latest committed (ONLINE) segment to CONSUMING if exists in the
   * ideal state.
   */
  private void removeNewConsumingSegment(Map<String, Map<String, String>> instanceStatesMap, String consumingSegment,
      @Nullable String latestCommittedSegment) {
    // Consuming segment should have all instances in CONSUMING state
    Map<String, String> consumingSegmentInstanceStateMap = instanceStatesMap.remove(consumingSegment);
    assertNotNull(consumingSegmentInstanceStateMap);
    assertEquals(new HashSet<>(consumingSegmentInstanceStateMap.values()),
        Collections.singleton(RealtimeSegmentOnlineOfflineStateModel.CONSUMING));

    if (latestCommittedSegment != null) {
      Map<String, String> latestCommittedSegmentInstanceStateMap = instanceStatesMap.get(latestCommittedSegment);
      assertNotNull(latestCommittedSegmentInstanceStateMap);
      for (Map.Entry<String, String> entry : latestCommittedSegmentInstanceStateMap.entrySet()) {
        // Latest committed segment should have all instances in ONLINE state
        assertEquals(entry.getValue(), RealtimeSegmentOnlineOfflineStateModel.ONLINE);
        entry.setValue(RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      }
    }
  }

  /**
   * Turns all instances for the new CONSUMING segment to OFFLINE in the ideal state.
   */
  private void turnNewConsumingSegmentOffline(Map<String, Map<String, String>> instanceStatesMap,
      String consumingSegment) {
    Map<String, String> consumingSegmentInstanceStateMap = instanceStatesMap.get(consumingSegment);
    assertNotNull(consumingSegmentInstanceStateMap);
    for (Map.Entry<String, String> entry : consumingSegmentInstanceStateMap.entrySet()) {
      // Consuming segment should have all instances in CONSUMING state
      assertEquals(entry.getValue(), RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      entry.setValue(RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
    }
  }

  private void testRepairs(FakePinotLLCRealtimeSegmentManager segmentManager) {
    Map<String, Map<String, String>> oldInstanceStatesMap =
        cloneInstanceStatesMap(segmentManager._idealState.getRecord().getMapFields());
    segmentManager._exceededMaxSegmentCompletionTime = false;
    segmentManager.ensureAllPartitionsConsuming();
    verifyNoChangeToOldEntries(segmentManager, oldInstanceStatesMap);
    segmentManager._exceededMaxSegmentCompletionTime = true;
    segmentManager.ensureAllPartitionsConsuming();
    verifyRepairs(segmentManager);
  }

  /**
   * Verifies that all entries in old ideal state are unchanged in the new ideal state (repair during the segment
   * completion). There could be new entries in the ideal state if all instances are OFFLINE for the latest segment.
   */
  private void verifyNoChangeToOldEntries(FakePinotLLCRealtimeSegmentManager segmentManager,
      Map<String, Map<String, String>> oldInstanceStatesMap) {
    Map<String, Map<String, String>> newInstanceStatesMap = segmentManager._idealState.getRecord().getMapFields();
    for (Map.Entry<String, Map<String, String>> entry : oldInstanceStatesMap.entrySet()) {
      String segmentName = entry.getKey();
      assertTrue(newInstanceStatesMap.containsKey(segmentName));
      assertEquals(newInstanceStatesMap.get(segmentName), entry.getValue());
    }
  }

  private void verifyRepairs(FakePinotLLCRealtimeSegmentManager segmentManager) {
    Map<String, Map<String, String>> instanceStatesMap = segmentManager._idealState.getRecord().getMapFields();

    // Segments are the same for ideal state and ZK metadata
    assertEquals(instanceStatesMap.keySet(), segmentManager._segmentZKMetadataMap.keySet());

    // Gather the ONLINE/CONSUMING segments for each partition ordered by sequence number
    List<Map<Integer, String>> partitionIdToSegmentsMap = new ArrayList<>(segmentManager._numPartitions);
    for (int partitionId = 0; partitionId < segmentManager._numPartitions; partitionId++) {
      partitionIdToSegmentsMap.add(new TreeMap<>());
    }
    for (Map.Entry<String, Map<String, String>> entry : instanceStatesMap.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();

      // Skip segments with all instances OFFLINE
      if (instanceStateMap.containsValue(RealtimeSegmentOnlineOfflineStateModel.ONLINE) || instanceStateMap
          .containsValue(RealtimeSegmentOnlineOfflineStateModel.CONSUMING)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        int partitionsId = llcSegmentName.getPartitionId();
        Map<Integer, String> sequenceNumberToSegmentMap = partitionIdToSegmentsMap.get(partitionsId);
        int sequenceNumber = llcSegmentName.getSequenceNumber();
        assertFalse(sequenceNumberToSegmentMap.containsKey(sequenceNumber));
        sequenceNumberToSegmentMap.put(sequenceNumber, segmentName);
      }
    }

    for (int partitionId = 0; partitionId < segmentManager._numPartitions; partitionId++) {
      List<String> segments = new ArrayList<>(partitionIdToSegmentsMap.get(partitionId).values());
      assertFalse(segments.isEmpty());
      int numSegments = segments.size();

      String latestSegment = segments.get(numSegments - 1);

      // Latest segment should have CONSUMING instance but no ONLINE instance in ideal state
      Map<String, String> instanceStateMap = instanceStatesMap.get(latestSegment);
      assertTrue(instanceStateMap.containsValue(RealtimeSegmentOnlineOfflineStateModel.CONSUMING));
      assertFalse(instanceStateMap.containsValue(RealtimeSegmentOnlineOfflineStateModel.ONLINE));

      // Latest segment ZK metadata should be IN_PROGRESS
      assertEquals(segmentManager._segmentZKMetadataMap.get(latestSegment).getStatus(), Status.IN_PROGRESS);

      for (int i = 0; i < numSegments - 1; i++) {
        String segmentName = segments.get(i);

        // Committed segment should have all instances in ONLINE state
        instanceStateMap = instanceStatesMap.get(segmentName);
        assertEquals(new HashSet<>(instanceStateMap.values()),
            Collections.singleton(RealtimeSegmentOnlineOfflineStateModel.ONLINE));

        // Committed segment ZK metadata should be DONE
        LLCRealtimeSegmentZKMetadata segmentZKMetadata = segmentManager._segmentZKMetadataMap.get(segmentName);
        assertEquals(segmentZKMetadata.getStatus(), Status.DONE);

        // Verify segment start/end offset
        assertEquals(segmentZKMetadata.getStartOffset(), PARTITION_OFFSET + i * (long) NUM_DOCS);
        assertEquals(segmentZKMetadata.getEndOffset(),
            segmentManager._segmentZKMetadataMap.get(segments.get(i + 1)).getStartOffset());
      }
    }
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testPreExistingSegments() {
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager();
    segmentManager._numReplicas = 2;
    segmentManager.makeTableConfig();
    segmentManager._numInstances = 5;
    segmentManager.makeConsumingInstancePartitions();
    segmentManager._numPartitions = 4;

    String existingSegmentName = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    segmentManager._segmentZKMetadataMap.put(existingSegmentName, new LLCRealtimeSegmentZKMetadata());
    segmentManager.setUpNewTable();
  }

  @Test
  public void testCommitSegmentWhenControllerWentThroughGC() {
    // Set up a new table with 2 replicas, 5 instances, 4 partitions
    FakePinotLLCRealtimeSegmentManager segmentManager1 =
        new FakePinotLLCRealtimeSegmentManagerII(FakePinotLLCRealtimeSegmentManagerII.Scenario.ZK_VERSION_CHANGED);
    setUpNewTable(segmentManager1, 2, 5, 4);
    FakePinotLLCRealtimeSegmentManager segmentManager2 =
        new FakePinotLLCRealtimeSegmentManagerII(FakePinotLLCRealtimeSegmentManagerII.Scenario.METADATA_STATUS_CHANGED);
    setUpNewTable(segmentManager2, 2, 5, 4);

    // Commit a segment for partition 0
    String committingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    CommittingSegmentDescriptor committingSegmentDescriptor =
        new CommittingSegmentDescriptor(committingSegment, PARTITION_OFFSET + NUM_DOCS, 0L);
    committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());

    try {
      segmentManager1.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      segmentManager2.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testCommitSegmentFile()
      throws Exception {
    PinotFSFactory.init(new BaseConfiguration());
    File tableDir = new File(TEMP_DIR, RAW_TABLE_NAME);
    String segmentName = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    String segmentFileName = SegmentCompletionUtils.generateSegmentFileName(segmentName);
    File segmentFile = new File(tableDir, segmentFileName);
    FileUtils.write(segmentFile, "temporary file contents");

    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager();
    String segmentLocation = SCHEME + tableDir + "/" + segmentFileName;
    CommittingSegmentDescriptor committingSegmentDescriptor =
        new CommittingSegmentDescriptor(segmentName, PARTITION_OFFSET, 0, segmentLocation);
    segmentManager.commitSegmentFile(REALTIME_TABLE_NAME, committingSegmentDescriptor);
    assertFalse(segmentFile.exists());
  }

  @Test
  public void testSegmentAlreadyThereAndExtraneousFilesDeleted()
      throws Exception {
    PinotFSFactory.init(new BaseConfiguration());
    File tableDir = new File(TEMP_DIR, RAW_TABLE_NAME);
    String segmentName = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    String otherSegmentName = new LLCSegmentName(RAW_TABLE_NAME, 1, 0, CURRENT_TIME_MS).getSegmentName();
    String segmentFileName = SegmentCompletionUtils.generateSegmentFileName(segmentName);
    String extraSegmentFileName = SegmentCompletionUtils.generateSegmentFileName(segmentName);
    String otherSegmentFileName = SegmentCompletionUtils.generateSegmentFileName(otherSegmentName);
    File segmentFile = new File(tableDir, segmentFileName);
    File extraSegmentFile = new File(tableDir, extraSegmentFileName);
    File otherSegmentFile = new File(tableDir, otherSegmentFileName);
    FileUtils.write(segmentFile, "temporary file contents");
    FileUtils.write(extraSegmentFile, "temporary file contents");
    FileUtils.write(otherSegmentFile, "temporary file contents");

    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager();
    String segmentLocation = SCHEME + tableDir + "/" + segmentFileName;
    CommittingSegmentDescriptor committingSegmentDescriptor =
        new CommittingSegmentDescriptor(segmentName, PARTITION_OFFSET, 0, segmentLocation);
    segmentManager.commitSegmentFile(REALTIME_TABLE_NAME, committingSegmentDescriptor);
    assertFalse(segmentFile.exists());
    assertFalse(extraSegmentFile.exists());
    assertTrue(otherSegmentFile.exists());
  }

  @Test
  public void testStopSegmentManager()
      throws Exception {
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager();
    segmentManager._numReplicas = 2;
    segmentManager.makeTableConfig();
    segmentManager._numInstances = 5;
    segmentManager.makeConsumingInstancePartitions();
    segmentManager._numPartitions = 4;
    segmentManager.stop();

    // All operations should fail after stopping the segment manager
    try {
      segmentManager.setUpNewTable(segmentManager._tableConfig, new IdealState(REALTIME_TABLE_NAME));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      segmentManager.removeLLCSegments(new IdealState(REALTIME_TABLE_NAME));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      segmentManager.commitSegmentFile(REALTIME_TABLE_NAME, mock(CommittingSegmentDescriptor.class));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, mock(CommittingSegmentDescriptor.class));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      segmentManager.segmentStoppedConsuming(new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS),
          Helix.PREFIX_OF_SERVER_INSTANCE + 0);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      segmentManager.ensureAllPartitionsConsuming(segmentManager._tableConfig, segmentManager._streamConfig);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Fake classes
  /////////////////////////////////////////////////////////////////////////////////

  private static class FakePinotLLCRealtimeSegmentManager extends PinotLLCRealtimeSegmentManager {
    static final ControllerConf CONTROLLER_CONF = new ControllerConf();

    static {
      CONTROLLER_CONF.setDataDir(TEMP_DIR.toString());
    }

    int _numReplicas;
    TableConfig _tableConfig;
    PartitionLevelStreamConfig _streamConfig;
    int _numInstances;
    InstancePartitions _consumingInstancePartitions;
    Map<String, LLCRealtimeSegmentZKMetadata> _segmentZKMetadataMap = new HashMap<>();
    Map<String, Integer> _segmentZKMetadataVersionMap = new HashMap<>();
    IdealState _idealState;
    int _numPartitions;
    boolean _exceededMaxSegmentCompletionTime = false;

    FakePinotLLCRealtimeSegmentManager() {
      super(mock(PinotHelixResourceManager.class), CONTROLLER_CONF, mock(ControllerMetrics.class));
    }

    void makeTableConfig() {
      Map<String, String> streamConfigs = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();
      _tableConfig =
          new TableConfig.Builder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(_numReplicas)
              .setLLC(true).setStreamConfigs(streamConfigs).build();
      _streamConfig = new PartitionLevelStreamConfig(_tableConfig);
    }

    void makeConsumingInstancePartitions() {
      List<String> instances = new ArrayList<>(_numInstances);
      for (int i = 0; i < _numInstances; i++) {
        instances.add(Helix.PREFIX_OF_SERVER_INSTANCE + i);
      }
      _consumingInstancePartitions =
          new InstancePartitions(InstancePartitionsType.CONSUMING.getInstancePartitionsName(RAW_TABLE_NAME));
      _consumingInstancePartitions.setInstances(0, 0, instances);
    }

    public void setUpNewTable() {
      setUpNewTable(_tableConfig, new IdealState(REALTIME_TABLE_NAME));
    }

    public void ensureAllPartitionsConsuming() {
      ensureAllPartitionsConsuming(_tableConfig, _streamConfig, _idealState, _numPartitions);
    }

    @Override
    TableConfig getTableConfig(String realtimeTableName) {
      return _tableConfig;
    }

    @Override
    InstancePartitions getConsumingInstancePartitions(TableConfig tableConfig) {
      return _consumingInstancePartitions;
    }

    @Override
    List<String> getAllSegments(String realtimeTableName) {
      return new ArrayList<>(_segmentZKMetadataMap.keySet());
    }

    @Override
    List<String> getLLCSegments(String realtimeTableName) {
      return new ArrayList<>(_segmentZKMetadataMap.keySet());
    }

    @Override
    LLCRealtimeSegmentZKMetadata getSegmentZKMetadata(String realtimeTableName, String segmentName,
        @Nullable Stat stat) {
      Preconditions.checkState(_segmentZKMetadataMap.containsKey(segmentName));
      if (stat != null) {
        stat.setVersion(_segmentZKMetadataVersionMap.get(segmentName));
      }
      return new LLCRealtimeSegmentZKMetadata(_segmentZKMetadataMap.get(segmentName).toZNRecord());
    }

    @Override
    void persistSegmentZKMetadata(String realtimeTableName, LLCRealtimeSegmentZKMetadata segmentZKMetadata,
        int expectedVersion) {
      String segmentName = segmentZKMetadata.getSegmentName();
      int version = _segmentZKMetadataVersionMap.getOrDefault(segmentName, -1);
      if (expectedVersion != -1) {
        Preconditions.checkState(expectedVersion == version);
      }
      _segmentZKMetadataMap.put(segmentName, segmentZKMetadata);
      _segmentZKMetadataVersionMap.put(segmentName, version + 1);
    }

    @Override
    protected IdealState getIdealState(String realtimeTableName) {
      return _idealState;
    }

    @Override
    protected void setIdealState(String realtimeTableName, IdealState idealState) {
      _idealState = idealState;
    }

    @Override
    void updateIdealStateOnSegmentCompletion(String realtimeTableName, String committingSegmentName,
        String newSegmentName, SegmentAssignment segmentAssignment,
        Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
      updateInstanceStatesForNewConsumingSegment(_idealState.getRecord().getMapFields(), committingSegmentName,
          newSegmentName, segmentAssignment, instancePartitionsMap);
    }

    @Override
    int getNumPartitions(StreamConfig streamConfig) {
      return _numPartitions;
    }

    @Override
    long getPartitionOffset(StreamConfig streamConfig, OffsetCriteria offsetCriteria, int partitionId) {
      // The criteria for this test should always be SMALLEST (for default streaming config and new added partitions)
      assertTrue(offsetCriteria.isSmallest());
      return PARTITION_OFFSET;
    }

    @Override
    boolean isExceededMaxSegmentCompletionTime(String realtimeTableName, String segmentName, long currentTimeMs) {
      return _exceededMaxSegmentCompletionTime;
    }

    @Override
    long getCurrentTimeMs() {
      return CURRENT_TIME_MS;
    }
  }

  private static class FakePinotLLCRealtimeSegmentManagerII extends FakePinotLLCRealtimeSegmentManager {
    enum Scenario {
      ZK_VERSION_CHANGED, METADATA_STATUS_CHANGED
    }

    final Scenario _scenario;

    FakePinotLLCRealtimeSegmentManagerII(Scenario scenario) {
      super();
      _scenario = scenario;
    }

    @Override
    LLCRealtimeSegmentZKMetadata getSegmentZKMetadata(String realtimeTableName, String segmentName,
        @Nullable Stat stat) {
      LLCRealtimeSegmentZKMetadata segmentZKMetadata = super.getSegmentZKMetadata(realtimeTableName, segmentName, stat);
      switch (_scenario) {
        case ZK_VERSION_CHANGED:
          // Mock another controller updated the segment ZK metadata during the process
          if (stat != null) {
            persistSegmentZKMetadata(realtimeTableName, segmentZKMetadata, stat.getVersion());
          }
          break;
        case METADATA_STATUS_CHANGED:
          // Mock another controller has updated the status of the segment ZK metadata
          segmentZKMetadata.setStatus(Status.DONE);
          break;
      }
      return segmentZKMetadata;
    }
  }
}

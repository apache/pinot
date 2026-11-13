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
package org.apache.pinot.controller.helix.core.assignment.segment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.PartitionAssignmentUtils;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests stable leader election and full replica placement for consuming replicate-mode segments.
public class ReplicateModeSegmentAssignmentTest {
  private static final String TABLE_NAME = "testTable_REALTIME";
  private static final List<String> SERVERS = List.of("s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8");

  @DataProvider
  public Object[][] layouts() {
    return new Object[][]{{1, 1}, {3, 1}, {3, 3}};
  }

  @Test(dataProvider = "layouts")
  public void testBalancedLeadersAcrossAssignmentLayouts(int numReplicaGroups, int numPartitions) {
    TableConfig tableConfig = tableConfig(true, 3);
    SegmentAssignment assignment = assignment(tableConfig);
    IdealState idealState = new IdealState(TABLE_NAME);
    Map<InstancePartitionsType, InstancePartitions> partitions =
        Map.of(InstancePartitionsType.CONSUMING, instancePartitions(numReplicaGroups, numPartitions));
    Map<String, Integer> counts = new HashMap<>();
    for (int partitionId = 0; partitionId < 9; partitionId++) {
      SegmentZKMetadata metadata = metadata(partitionId, 0, true);
      List<String> instances = assignment.assignConsumingSegment(metadata, idealState, partitions);
      assertEquals(instances.size(), 1);
      String leader = instances.get(0);
      assertEquals(ReplicateModeAssignmentUtils.getLeader(idealState, partitionId), leader);
      List<String> replicas = ReplicateModeAssignmentUtils.getReplicas(idealState, partitionId);
      assertEquals(replicas.size(), 3);
      assertTrue(replicas.contains(leader));
      assertEquals(ReplicateModeAssignmentUtils.getFollowers(idealState, partitionId).size(), 2);
      counts.merge(leader, 1, Integer::sum);
      idealState.setPartitionState(metadata.getSegmentName(), leader, "CONSUMING");
      assertEquals(idealState.getInstanceStateMap(metadata.getSegmentName()), Map.of(leader, "CONSUMING"));
    }
    assertEquals(counts.size(), 9);
    assertEquals(new HashSet<>(counts.values()), Set.of(1));
  }

  @Test
  public void testLeaderSurvivesNewSegmentsRetriesAndControllerRestart() {
    TableConfig tableConfig = tableConfig(true, 3);
    Map<InstancePartitionsType, InstancePartitions> partitions =
        Map.of(InstancePartitionsType.CONSUMING, instancePartitions(1, 1));
    IdealState idealState = new IdealState(TABLE_NAME);
    String leader = assignment(tableConfig).assignConsumingSegment(metadata(0, 0, true), idealState, partitions).get(0);
    List<String> replicas = ReplicateModeAssignmentUtils.getReplicas(idealState, 0);
    // A skewed load must not displace a valid existing leader at the next segment boundary.
    for (int partitionId = 10; partitionId < 15; partitionId++) {
      ReplicateModeAssignmentUtils.setAssignment(idealState, partitionId, leader, replicas);
    }
    IdealState restarted = new IdealState(new ZNRecord(idealState.getRecord()));
    SegmentAssignment afterRestart = assignment(tableConfig);
    assertEquals(afterRestart.assignConsumingSegment(metadata(0, 1, true), restarted, partitions), List.of(leader));
    assertEquals(afterRestart.assignConsumingSegment(metadata(0, 1, true), restarted, partitions), List.of(leader));
    assertEquals(ReplicateModeAssignmentUtils.getLeaders(restarted).size(), 6);
    assertEquals(ReplicateModeAssignmentUtils.getReplicas(restarted, 0), replicas);
  }

  @Test
  public void testSegmentModeOverridesCurrentTableConfiguration() {
    Map<InstancePartitionsType, InstancePartitions> partitions =
        Map.of(InstancePartitionsType.CONSUMING, instancePartitions(1, 1));
    for (boolean segmentMode : List.of(false, true)) {
      IdealState idealState = new IdealState(TABLE_NAME);
      SegmentAssignment assignment = assignment(tableConfig(!segmentMode, 3));
      List<String> instances = assignment.assignConsumingSegment(metadata(0, 0, segmentMode), idealState, partitions);
      assertEquals(instances.size(), segmentMode ? 1 : 3);
      if (!segmentMode) {
        assertNull(ReplicateModeAssignmentUtils.getLeader(idealState, 0));
        assertEquals(idealState.getRecord().getListFields(), Map.of());
      }
    }
  }

  @Test
  public void testCompletedSegmentsStillUseFullReplicaAssignment() {
    TableConfig tableConfig = tableConfig(true, 3);
    SegmentAssignment assignment = assignment(tableConfig);
    IdealState idealState = new IdealState(TABLE_NAME);
    SegmentZKMetadata metadata = metadata(0, 0, true);
    assignment.assignConsumingSegment(metadata, idealState,
        Map.of(InstancePartitionsType.CONSUMING, instancePartitions(1, 1)));
    List<String> replicas = ReplicateModeAssignmentUtils.getReplicas(idealState, 0);
    for (InstancePartitionsType type : List.of(InstancePartitionsType.CONSUMING, InstancePartitionsType.COMPLETED)) {
      assertEquals(assignment.assignSegment(metadata.getSegmentName(), idealState,
          Map.of(type, instancePartitions(1, 1))).size(), 3);
      assertEquals(ReplicateModeAssignmentUtils.getReplicas(idealState, 0), replicas);
    }
  }

  @Test
  public void testSingleReplicaHasNoFollowers() {
    IdealState idealState = new IdealState(TABLE_NAME);
    List<String> instances = assignment(tableConfig(true, 1)).assignConsumingSegment(metadata(0, 0, true), idealState,
        Map.of(InstancePartitionsType.CONSUMING, instancePartitions(1, 1)));
    assertEquals(instances.size(), 1);
    assertEquals(ReplicateModeAssignmentUtils.getReplicas(idealState, 0), instances);
    assertEquals(ReplicateModeAssignmentUtils.getFollowers(idealState, 0), List.of());
  }

  @Test
  public void testEnablingPartitionPinningRetainsFullReplicaSet() {
    TableConfig tableConfig = tableConfig(true, 3);
    IdealState idealState = new IdealState(TABLE_NAME);
    Map<InstancePartitionsType, InstancePartitions> partitions =
        Map.of(InstancePartitionsType.CONSUMING, instancePartitions(3, 1));
    SegmentZKMetadata metadata = metadata(0, 0, true);
    String leader = assignment(tableConfig).assignConsumingSegment(metadata, idealState, partitions).get(0);
    idealState.setPartitionState(metadata.getSegmentName(), leader, "CONSUMING");
    List<String> replicas = ReplicateModeAssignmentUtils.getReplicas(idealState, 0);
    tableConfig.setRoutingConfig(
        new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, null));
    assertEquals(assignment(tableConfig).assignConsumingSegment(metadata(0, 1, true), idealState, partitions),
        List.of(leader));
    assertEquals(PartitionAssignmentUtils.getInstances(idealState, 0), replicas);
    assertEquals(ReplicateModeAssignmentUtils.getReplicas(idealState, 0), replicas);
  }

  @Test
  public void testMultiStreamPartitionsHaveSeparateRoleRecords() {
    TableConfig tableConfig = tableConfig(true, 3);
    StreamIngestionConfig oldConfig = tableConfig.getIngestionConfig().getStreamIngestionConfig();
    Map<String, String> secondStream = new HashMap<>(oldConfig.getStreamConfigMaps().get(0));
    secondStream.put("stream.fakeStream.topic.name", "secondTopic");
    StreamIngestionConfig streamConfig =
        new StreamIngestionConfig(List.of(oldConfig.getStreamConfigMaps().get(0), secondStream));
    streamConfig.setReplicateMode(true);
    tableConfig.getIngestionConfig().setStreamIngestionConfig(streamConfig);
    IdealState idealState = new IdealState(TABLE_NAME);
    Map<InstancePartitionsType, InstancePartitions> partitions =
        Map.of(InstancePartitionsType.CONSUMING, instancePartitions(1, 1));
    SegmentAssignment assignment = assignment(tableConfig);
    assignment.assignConsumingSegment(metadata(0, 0, true), idealState, partitions);
    int secondPartition = IngestionConfigUtils.PARTITION_PADDING_OFFSET;
    assignment.assignConsumingSegment(metadata(secondPartition, 0, true), idealState, partitions);
    assertEquals(ReplicateModeAssignmentUtils.getLeaders(idealState).keySet(), Set.of(0, secondPartition));
    assertEquals(ReplicateModeAssignmentUtils.getReplicas(idealState, 0),
        ReplicateModeAssignmentUtils.getReplicas(idealState, secondPartition));
    assertNotEquals(ReplicateModeAssignmentUtils.getLeader(idealState, 0),
        ReplicateModeAssignmentUtils.getLeader(idealState, secondPartition));
  }

  private static SegmentAssignment assignment(TableConfig tableConfig) {
    return SegmentAssignmentFactory.getSegmentAssignment(mock(HelixManager.class), tableConfig);
  }

  private static TableConfig tableConfig(boolean replicateMode, int replication) {
    StreamIngestionConfig streamConfig = new StreamIngestionConfig(
        List.of(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap()));
    streamConfig.setReplicateMode(replicateMode);
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(streamConfig);
    return new TableConfigBuilder(TableType.REALTIME).setTableName("testTable").setNumReplicas(replication)
        .setIngestionConfig(ingestionConfig).build();
  }

  private static SegmentZKMetadata metadata(int partitionId, int sequence, boolean replicateMode) {
    SegmentZKMetadata metadata = new SegmentZKMetadata(
        new LLCSegmentName("testTable", partitionId, sequence, 0).getSegmentName());
    metadata.setStatus(Status.IN_PROGRESS);
    metadata.setReplicateMode(replicateMode);
    return metadata;
  }

  private static InstancePartitions instancePartitions(int numReplicaGroups, int numPartitions) {
    InstancePartitions partitions = new InstancePartitions("testTable_CONSUMING");
    if (numReplicaGroups == 1) {
      partitions.setInstances(0, 0, SERVERS);
    } else {
      for (int group = 0; group < numReplicaGroups; group++) {
        for (int partition = 0; partition < numPartitions; partition++) {
          List<String> instances = new ArrayList<>();
          for (int i = partition; i < 3; i += numPartitions) {
            instances.add(SERVERS.get(group * 3 + i));
          }
          partitions.setInstances(partition, group, instances);
        }
      }
    }
    return partitions;
  }
}

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
package org.apache.pinot.common.assignment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceConstraintConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class InstancePartitionsUtilsTest {

  @Test
  public void testShouldFetchPreConfiguredInstancePartitions() {
    Map<InstancePartitionsType, String> instancePartitionsMap = new HashMap<>();
    instancePartitionsMap.put(InstancePartitionsType.OFFLINE, "testPartitions");
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put(InstancePartitionsType.OFFLINE.name(),
        getInstanceAssignmentConfig(InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstancePartitionsMap(instancePartitionsMap)
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .build();

    Assert.assertTrue(InstancePartitionsUtils.shouldFetchPreConfiguredInstancePartitions(tableConfig,
        InstancePartitionsType.OFFLINE));
  }

  @Test
  public void testShouldFetchPreConfiguredInstancePartitionsMirrorServerSet() {
    Map<InstancePartitionsType, String> instancePartitionsMap = new HashMap<>();
    instancePartitionsMap.put(InstancePartitionsType.OFFLINE, "testPartitions");
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put(InstancePartitionsType.OFFLINE.name(),
        getInstanceAssignmentConfig(InstanceAssignmentConfig.PartitionSelector.MIRROR_SERVER_SET_PARTITION_SELECTOR));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstancePartitionsMap(instancePartitionsMap)
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .build();

    Assert.assertFalse(InstancePartitionsUtils.shouldFetchPreConfiguredInstancePartitions(tableConfig,
        InstancePartitionsType.OFFLINE));
  }

  @Test
  public void testGetPartitionIdAndReplicaGroupId() {
    Assert.assertEquals(InstancePartitionsUtils.getPartitionIdAndReplicaGroupId("0_0"), Pair.of(0, 0));
    Assert.assertEquals(InstancePartitionsUtils.getPartitionIdAndReplicaGroupId("1_0"), Pair.of(1, 0));
    Assert.assertEquals(InstancePartitionsUtils.getPartitionIdAndReplicaGroupId("0_1"), Pair.of(0, 1));
  }

  @Test
  public void testReplaceInstancePartitionsInIdealState() {
    IdealState idealState = new IdealState("testTable");
    List<InstancePartitions> instancePartitionsList = new ArrayList<>();
    instancePartitionsList.add(
        new InstancePartitions("testTable_CONSUMING",
            Map.of("0_0", List.of("instance-0", "instance-1"), "0_1", List.of("instance-2", "instance-3")))
    );
    instancePartitionsList.add(
        new InstancePartitions("testTable_COMPLETED", Map.of("0_0", List.of("instance-4")))
    );

    InstancePartitionsUtils.replaceInstancePartitionsInIdealState(idealState, instancePartitionsList);
    Map<String, List<String>> listFields = idealState.getRecord().getListFields();

    List<String> consumingPartition0Rg0 = listFields.get(
        InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + "testTable_CONSUMING"
            + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_0");
    Assert.assertNotNull(consumingPartition0Rg0);
    Assert.assertEquals(new HashSet<>(consumingPartition0Rg0), Set.of("instance-0", "instance-1"));

    List<String> consumingPartition0Rg1 = listFields.get(
        InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + "testTable_CONSUMING"
            + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_1");
    Assert.assertNotNull(consumingPartition0Rg1);
    Assert.assertEquals(new HashSet<>(consumingPartition0Rg1), Set.of("instance-2", "instance-3"));

    List<String> completedPartition0Rg0 = listFields.get(
        InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + "testTable_COMPLETED"
            + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_0");
    Assert.assertNotNull(completedPartition0Rg0);
    Assert.assertEquals(completedPartition0Rg0, List.of("instance-4"));

    // Add one more instance to 0_0 in COMPLETED, replace existing instance
    List<InstancePartitions> updatedInstancePartitionsList = List.of(instancePartitionsList.get(0),
        new InstancePartitions("testTable_COMPLETED", Map.of("0_0", List.of("instance-5", "instance-6"))));
    InstancePartitionsUtils.replaceInstancePartitionsInIdealState(idealState, updatedInstancePartitionsList);
    // Verify idempotent
    InstancePartitionsUtils.replaceInstancePartitionsInIdealState(idealState, updatedInstancePartitionsList);

    // Ensure that CONSUMING instance partitions aren't affected
    consumingPartition0Rg0 = listFields.get(
        InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + "testTable_CONSUMING"
            + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_0");
    Assert.assertNotNull(consumingPartition0Rg0);
    Assert.assertEquals(new HashSet<>(consumingPartition0Rg0), Set.of("instance-0", "instance-1"));

    consumingPartition0Rg1 = listFields.get(
        InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + "testTable_CONSUMING"
            + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_1");
    Assert.assertNotNull(consumingPartition0Rg1);
    Assert.assertEquals(new HashSet<>(consumingPartition0Rg1), Set.of("instance-2", "instance-3"));

    // Verify that COMPLETED instance partitions 0_0 now contains the new instances.
    completedPartition0Rg0 = listFields.get(
        InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + "testTable_COMPLETED"
            + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_0");
    Assert.assertNotNull(completedPartition0Rg0);
    Assert.assertEquals(new HashSet<>(completedPartition0Rg0), Set.of("instance-5", "instance-6"));
  }

  @Test
  public void testServerToReplicaGroupMap() {
    String ipName = InstancePartitionsUtils.getInstancePartitionsName("testTable_OFFLINE", "OFFLINE");
    ZNRecord znRecord = new ZNRecord("testTable_OFFLINE");
    // Keys:
    // 0_0 -> A,B
    // 0_1 -> C
    // 1_0 -> B
    // 1_1 -> A
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + ipName
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_0", List.of("A", "B"));
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + ipName
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_1", List.of("C", "D"));
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + ipName
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "1_0", List.of("E", "F"));
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + ipName
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "1_1", List.of("G", "H"));
    IdealState is = new IdealState(znRecord);

    Map<String, Integer> map = InstancePartitionsUtils.serverToReplicaGroupMap(is);
    // Each server should map to its replica group id
    assertEquals(map.get("A").intValue(), 0);
    assertEquals(map.get("B").intValue(), 0);
    assertEquals(map.get("C").intValue(), 1);
    assertEquals(map.get("D").intValue(), 1);
    assertEquals(map.get("E").intValue(), 0);
    assertEquals(map.get("F").intValue(), 0);
    assertEquals(map.get("G").intValue(), 1);
    assertEquals(map.get("H").intValue(), 1);
  }

  @Test
  public void testExtractInstancePartitionsFromIdealState() {
    ZNRecord znRecord = new ZNRecord("testTable_REALTIME");
    // Add CONSUMING instance partitions
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + "testTable_CONSUMING"
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_0", List.of("instance-0", "instance-1"));
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + "testTable_CONSUMING"
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_1", List.of("instance-2", "instance-3"));
    // Add COMPLETED instance partitions
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + "testTable_COMPLETED"
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_0", List.of("instance-4", "instance-5"));
    // Add a non-instance-partitions list field (should be ignored)
    znRecord.setListField("k", List.of("v1", "v2", "v3"));
    IdealState idealState = new IdealState(znRecord);

    Map<String, InstancePartitions> result =
        InstancePartitionsUtils.extractInstancePartitionsFromIdealState(idealState);

    assertEquals(result.size(), 2);

    InstancePartitions consuming = result.get("testTable_CONSUMING");
    Assert.assertNotNull(consuming);
    assertEquals(consuming.getInstancePartitionsName(), "testTable_CONSUMING");
    assertEquals(consuming.getNumPartitions(), 1);
    assertEquals(consuming.getNumReplicaGroups(), 2);
    assertEquals(consuming.getInstances(0, 0), List.of("instance-0", "instance-1"));
    assertEquals(consuming.getInstances(0, 1), List.of("instance-2", "instance-3"));

    InstancePartitions completed = result.get("testTable_COMPLETED");
    Assert.assertNotNull(completed);
    assertEquals(completed.getInstancePartitionsName(), "testTable_COMPLETED");
    assertEquals(completed.getNumPartitions(), 1);
    assertEquals(completed.getNumReplicaGroups(), 1);
    assertEquals(completed.getInstances(0, 0), List.of("instance-4", "instance-5"));
  }

  @Test
  public void testExtractInstancePartitionsFromIdealStateWithTier() {
    ZNRecord znRecord = new ZNRecord("testTable_OFFLINE");
    // Add standard OFFLINE instance partitions
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + "testTable_OFFLINE"
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_0", List.of("instance-0", "instance-1"));
    // Add tier instance partitions (name contains __TIER__)
    String tierInstancePartitionsName =
        InstancePartitionsUtils.getInstancePartitionsNameForTier("testTable", "hotTier");
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + tierInstancePartitionsName
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_0", List.of("instance-2", "instance-3"));
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + tierInstancePartitionsName
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "1_0", List.of("instance-4", "instance-5"));
    IdealState idealState = new IdealState(znRecord);

    Map<String, InstancePartitions> result =
        InstancePartitionsUtils.extractInstancePartitionsFromIdealState(idealState);

    assertEquals(result.size(), 2);

    InstancePartitions offline = result.get("testTable_OFFLINE");
    Assert.assertNotNull(offline);
    assertEquals(offline.getInstancePartitionsName(), "testTable_OFFLINE");
    assertEquals(offline.getNumPartitions(), 1);
    assertEquals(offline.getNumReplicaGroups(), 1);
    assertEquals(offline.getInstances(0, 0), List.of("instance-0", "instance-1"));

    InstancePartitions tier = result.get(tierInstancePartitionsName);
    Assert.assertNotNull(tier);
    assertEquals(tier.getInstancePartitionsName(), tierInstancePartitionsName);
    assertEquals(tier.getNumPartitions(), 2);
    assertEquals(tier.getNumReplicaGroups(), 1);
    assertEquals(tier.getInstances(0, 0), List.of("instance-2", "instance-3"));
    assertEquals(tier.getInstances(1, 0), List.of("instance-4", "instance-5"));
  }

  private static InstanceAssignmentConfig getInstanceAssignmentConfig(InstanceAssignmentConfig.PartitionSelector
      partitionSelector) {
    InstanceTagPoolConfig instanceTagPoolConfig =
        new InstanceTagPoolConfig("tag", true, 1, null);
    List<String> constraints = new ArrayList<>();
    constraints.add("constraints1");
    InstanceConstraintConfig instanceConstraintConfig = new InstanceConstraintConfig(constraints);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 1, 1,
            1, 1, 1, true,
            null);
    return new InstanceAssignmentConfig(instanceTagPoolConfig, instanceConstraintConfig,
        instanceReplicaGroupPartitionConfig, partitionSelector.name(), false);
  }
}

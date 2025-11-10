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
  public void testCombineInstancePartitionsInIdealState() {
    IdealState idealState = new IdealState("testTable");
    List<InstancePartitions> instancePartitionsList = new ArrayList<>();
    instancePartitionsList.add(
        new InstancePartitions("testTable_CONSUMING",
            Map.of("0_0", List.of("instance-0", "instance-1"), "0_1", List.of("instance-2", "instance-3")))
    );
    instancePartitionsList.add(
        new InstancePartitions("testTable_COMPLETED", Map.of("0_0", List.of("instance-4")))
    );

    InstancePartitionsUtils.combineInstancePartitionsInIdealState(idealState, instancePartitionsList);
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
    InstancePartitionsUtils.combineInstancePartitionsInIdealState(idealState, updatedInstancePartitionsList);
    // Verify idempotent
    InstancePartitionsUtils.combineInstancePartitionsInIdealState(idealState, updatedInstancePartitionsList);

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

    // Verify that COMPLETED instance partitions 0_0 now contains both the old and new instances.
    completedPartition0Rg0 = listFields.get(
        InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + "testTable_COMPLETED"
            + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_0");
    Assert.assertNotNull(completedPartition0Rg0);
    Assert.assertEquals(new HashSet<>(completedPartition0Rg0), Set.of("instance-4", "instance-5", "instance-6"));
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

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
package org.apache.pinot.controller.helix.core.assignment.instance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;


public class InstanceAssignmentTest {
  private static final String RAW_TABLE_NAME = "myTable";
  private static final String TENANT_NAME = "tenant";
  private static final String OFFLINE_TAG = TagNameUtils.getOfflineTagForTenant(TENANT_NAME);
  private static final String SERVER_INSTANCE_ID_PREFIX = "Server_localhost_";

  @Test
  public void testDefaultOfflineReplicaGroup() {
    int numReplicas = 3;
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setServerTenant(TENANT_NAME)
            .setNumReplicas(numReplicas)
            .setSegmentAssignmentStrategy(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY).build();
    int numInstancesPerPartition = 2;
    tableConfig.getValidationConfig()
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(null, numInstancesPerPartition));
    InstanceAssignmentDriver driver = new InstanceAssignmentDriver(tableConfig);
    int numInstances = 10;
    List<InstanceConfig> instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      InstanceConfig instanceConfig = new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfigs.add(instanceConfig);
    }

    // Instances should be assigned to 3 replica-groups with a round-robin fashion, each with 2 instances
    InstancePartitions instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicas);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    // Instances of index 4 to 7 are not assigned because of the hash-based rotation
    // Math.abs("myTable_OFFLINE".hashCode()) % 10 = 8
    // [i8, i9, i0, i1, i2, i3, i4, i5, i6, i7]
    //  r0  r1  r2  r0  r1  r2
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 8));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 2, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 3));

    String partitionColumnName = "partition";
    int numPartitions = 2;
    tableConfig.getValidationConfig()
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(partitionColumnName, numInstancesPerPartition));
    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(
        Collections.singletonMap(partitionColumnName, new ColumnPartitionConfig("Modulo", numPartitions)));
    tableConfig.getIndexingConfig().setSegmentPartitionConfig(segmentPartitionConfig);

    // Instances should be assigned to 3 replica-groups with a round-robin fashion, each with 3 instances, then these 3
    // instances should be assigned to 2 partitions, each with 2 instances
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicas);
    assertEquals(instancePartitions.getNumPartitions(), numPartitions);
    // Instance of index 7 is not assigned because of the hash-based rotation
    // Math.abs("myTable_OFFLINE".hashCode()) % 10 = 8
    // [i8, i9, i0, i1, i2, i3, i4, i5, i6, i7]
    //  r0, r1, r2, r0, r1, r2, r0, r1, r2
    // r0: [i8, i1, i4]
    //      p0, p0, p1
    //      p1
    // r1: [i9, i2, i5]
    //      p0, p0, p1
    //      p1
    // r2: [i0, i3, i6]
    //      p0, p0, p1
    //      p1
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 8));
    assertEquals(instancePartitions.getInstances(1, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 8));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 2, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(1, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(1, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 6));
  }

  @Test
  public void testPoolBased() {
    // 10 instances in 2 pools, each with 5 instances
    int numInstances = 10;
    int numPools = 2;
    int numInstancesPerPool = numInstances / numPools;
    List<InstanceConfig> instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      InstanceConfig instanceConfig = new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i);
      instanceConfig.addTag(OFFLINE_TAG);
      int pool = i / numInstancesPerPool;
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }

    // Use all pools
    InstanceTagPoolConfig tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    // Assign to 2 replica-groups so that each replica-group is assigned to one pool
    int numReplicaGroups = numPools;
    InstanceReplicaGroupPartitionConfig replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, 0, 0, 0);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
            new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig))).build();
    InstanceAssignmentDriver driver = new InstanceAssignmentDriver(tableConfig);

    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // All instances in pool 0 should be assigned to replica-group 0, and all instances in pool 1 should be assigned to
    // replica-group 1
    InstancePartitions instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0), Arrays
        .asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4));
    assertEquals(instancePartitions.getInstances(0, 1), Arrays
        .asList(SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 6, SERVER_INSTANCE_ID_PREFIX + 7,
            SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 9));

    // Add the third pool with same number of instances
    numPools = 3;
    numInstances = numPools * numInstancesPerPool;
    for (int i = numInstances - numInstancesPerPool; i < numInstances; i++) {
      InstanceConfig instanceConfig = new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i);
      instanceConfig.addTag(OFFLINE_TAG);
      int pool = numPools - 1;
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }

    // Math.abs("myTable_OFFLINE".hashCode()) % 3 = 2
    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // Pool 0 and 2 will be selected in the pool selection
    // All instances in pool 0 should be assigned to replica-group 0, and all instances in pool 2 should be assigned to
    // replica-group 1
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0), Arrays
        .asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4));
    assertEquals(instancePartitions.getInstances(0, 1), Arrays
        .asList(SERVER_INSTANCE_ID_PREFIX + 10, SERVER_INSTANCE_ID_PREFIX + 11, SERVER_INSTANCE_ID_PREFIX + 12,
            SERVER_INSTANCE_ID_PREFIX + 13, SERVER_INSTANCE_ID_PREFIX + 14));

    // Select all 3 pools in pool selection
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig)));

    // Math.abs("myTable_OFFLINE".hashCode()) % 3 = 2
    // All instances in pool 2 should be assigned to replica-group 0, and all instances in pool 0 should be assigned to
    // replica-group 1
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0), Arrays
        .asList(SERVER_INSTANCE_ID_PREFIX + 10, SERVER_INSTANCE_ID_PREFIX + 11, SERVER_INSTANCE_ID_PREFIX + 12,
            SERVER_INSTANCE_ID_PREFIX + 13, SERVER_INSTANCE_ID_PREFIX + 14));
    assertEquals(instancePartitions.getInstances(0, 1), Arrays
        .asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4));

    // Select pool 0 and 1 in pool selection
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, 0, Arrays.asList(0, 1));
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig)));

    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // All instances in pool 0 should be assigned to replica-group 0, and all instances in pool 1 should be assigned to
    // replica-group 1
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0), Arrays
        .asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4));
    assertEquals(instancePartitions.getInstances(0, 1), Arrays
        .asList(SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 6, SERVER_INSTANCE_ID_PREFIX + 7,
            SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 9));

    // Assign instances from 2 pools to 3 replica-groups
    numReplicaGroups = numPools;
    replicaPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, 0, 0, 0);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig)));

    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // [pool0, pool1]
    //  r0     r1
    //  r2
    // Each replica-group should have 2 instances assigned
    // Math.abs("myTable_OFFLINE".hashCode()) % 5 = 3
    // pool 0: [i3, i4, i0, i1, i2]
    //          r0  r2  r0  r2
    // pool 1: [i8, i9, i5, i6, i7]
    //          r1  r1
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 4));
  }

  @Test
  public void testIllegalConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    InstanceAssignmentDriver driver = new InstanceAssignmentDriver(tableConfig);

    int numInstances = 10;
    List<InstanceConfig> instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      InstanceConfig instanceConfig = new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i);
      instanceConfigs.add(instanceConfig);
    }

    // No instance assignment config
    assertFalse(InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig, InstancePartitionsType.OFFLINE));
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Instance assignment is not allowed for the given table config");
    }

    InstanceTagPoolConfig tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, false, 0, null);
    InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(false, 0, 0, 0, 0, 0);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // No instance with correct tag
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "No enabled instance has the tag: tenant_OFFLINE");
    }

    for (InstanceConfig instanceConfig : instanceConfigs) {
      instanceConfig.addTag(OFFLINE_TAG);
    }

    // All instances should be assigned as replica-group 0 partition 0
    InstancePartitions instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
    assertEquals(instancePartitions.getNumReplicaGroups(), 1);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    List<String> expectedInstances = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      expectedInstances.add(SERVER_INSTANCE_ID_PREFIX + i);
    }
    assertEquals(instancePartitions.getInstances(0, 0), expectedInstances);

    // Enable pool
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, 0, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // No instance has correct pool configured
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "No enabled instance has the pool configured for the tag: tenant_OFFLINE");
    }

    for (int i = 0; i < numInstances; i++) {
      InstanceConfig instanceConfig = instanceConfigs.get(i);
      if (i < numInstances / 2) {
        instanceConfig.getRecord().setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, "0"));
      } else {
        instanceConfig.getRecord().setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, "1"));
      }
    }

    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // All instances in pool 0 should be assigned as replica-group 0 partition 0
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
    assertEquals(instancePartitions.getNumReplicaGroups(), 1);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    expectedInstances.clear();
    for (int i = 0; i < numInstances / 2; i++) {
      expectedInstances.add(SERVER_INSTANCE_ID_PREFIX + i);
    }
    assertEquals(instancePartitions.getInstances(0, 0), expectedInstances);

    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, 3, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Ask for too many pools
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Not enough instance pools (2 in the cluster, asked for 3)");
    }

    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, 0, Arrays.asList(0, 2));
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Ask for pool that does not exist
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Cannot find all instance pools configured: [0, 2]");
    }

    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, 0, null);
    replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(false, 6, 0, 0, 0, 0);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Ask for too many instances
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Not enough qualified instances from pool: 0 (5 in the pool, asked for 6)");
    }

    // Enable replica-group
    replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, 0, 0, 0, 0);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Number of replica-groups must be positive
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Number of replica-groups must be positive");
    }

    replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, 11, 0, 0, 0);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Ask for too many replica-groups
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Not enough qualified instances from pool: 0, cannot select 6 replica-groups from 5 instances");
    }

    replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, 3, 3, 0, 0);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Ask for too many instances
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Not enough qualified instances from pool: 0 (5 in the pool, asked for 6)");
    }

    replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, 3, 2, 0, 3);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Ask for too many instances per partition
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Number of instances per partition: 3 must be smaller or equal to number of instances per replica-group: 2");
    }

    replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, 3, 2, 0, 0);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Math.abs("myTable_OFFLINE".hashCode()) % 5 = 3
    // pool0: [i3, i4, i0, i1, i2]
    //         r0  r2  r0  r2
    // pool1: [i8, i9, i5, i6, i7]
    //         r1  r1
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs);
    assertEquals(instancePartitions.getNumReplicaGroups(), 3);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 4));
  }
}

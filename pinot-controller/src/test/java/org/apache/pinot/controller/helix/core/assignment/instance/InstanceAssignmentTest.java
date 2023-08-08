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
import java.util.LinkedList;
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
import org.apache.pinot.spi.config.table.assignment.InstanceConstraintConfig;
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
  private static final String SERVER_INSTANCE_POOL_PREFIX = "_pool_";
  private static final String TABLE_NAME_ZERO_HASH_COMPLEMENT = "12";

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
    InstancePartitions instancePartitions =
        driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicas);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    // Instances of index 4 to 7 are not assigned because of the hash-based rotation
    // Math.abs("myTable_OFFLINE".hashCode()) % 10 = 8
    // [i8, i9, i0, i1, i2, i3, i4, i5, i6, i7]
    //  r0  r1  r2  r0  r1  r2
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 3));

    String partitionColumnName = "partition";
    int numPartitions = 2;
    tableConfig.getValidationConfig()
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(partitionColumnName, numInstancesPerPartition));
    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(
        Collections.singletonMap(partitionColumnName, new ColumnPartitionConfig("Modulo", numPartitions, null)));
    tableConfig.getIndexingConfig().setSegmentPartitionConfig(segmentPartitionConfig);

    // Instances should be assigned to 3 replica-groups with a round-robin fashion, each with 3 instances, then these 3
    // instances should be assigned to 2 partitions, each with 2 instances
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
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
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(1, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 8));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(1, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(1, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 6, SERVER_INSTANCE_ID_PREFIX + 0));

    // ===== Test against the cases when the existing instancePartitions isn't null,
    //       and minimizeDataMovement is set to true. =====
    // Put the existing instancePartitions as the parameter to the InstanceAssignmentDriver.
    // The returned instance partition should be the same as the last computed one.
    tableConfig.getValidationConfig().setMinimizeDataMovement(true);

    // Instances should be assigned to 3 replica-groups with a round-robin fashion, each with 3 instances, then these 3
    // instances should be assigned to 2 partitions, each with 2 instances
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, instancePartitions);
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
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(1, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 8));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(1, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(1, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 6, SERVER_INSTANCE_ID_PREFIX + 0));

    // Remove two instances (i2, i6) and add two new instances (i10, i11).
    instanceConfigs.remove(6);
    instanceConfigs.remove(2);
    for (int i = numInstances; i < numInstances + 2; i++) {
      InstanceConfig instanceConfig = new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfigs.add(instanceConfig);
    }

    // Instances should be assigned to 3 replica-groups with a round-robin fashion, each with 3 instances, then these 3
    // instances should be assigned to 2 partitions, each with 2 instances
    // Leverage the latest instancePartitions from last computation as the parameter.
    // Data movement is minimized so that: i2 -> i10, i6 -> i11
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, instancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicas);
    assertEquals(instancePartitions.getNumPartitions(), numPartitions);

    // Instance of index 7 is not assigned because of the hash-based rotation
    // Math.abs("myTable_OFFLINE".hashCode()) % 10 = 8
    // [i8, i9, i0, i1, i10, i3, i4, i5, i11, i7]
    //  r0, r1, r2, r0, r1, r2, r0, r1, r2
    // r0: [i8, i1, i4]
    //      p0, p0, p1
    //      p1
    // r1: [i9, i10, i5]
    //      p0, p0, p1
    //      p1
    // r2: [i0, i3, i11]
    //      p0, p0, p1
    //      p1
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(1, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 8));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 10));
    assertEquals(instancePartitions.getInstances(1, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(1, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 11, SERVER_INSTANCE_ID_PREFIX + 0));

    // Add 2 more instances to the ZK and increase the number of instances per replica group from 2 to 3.
    for (int i = numInstances + 2; i < numInstances + 4; i++) {
      InstanceConfig instanceConfig = new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfigs.add(instanceConfig);
    }
    numInstancesPerPartition = 3;
    tableConfig.getValidationConfig()
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(partitionColumnName, numInstancesPerPartition));

    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, instancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicas);
    assertEquals(instancePartitions.getNumPartitions(), numPartitions);

    // Math.abs("myTable_OFFLINE".hashCode()) % 12 = 2
    // [i10, i11, i12, i13, i3, i4, i5, i11, i7, i8, i9, i0, i1]
    // For r0, the candidate instances are [i12, i13, i4, i7, i8, i1].
    //   For p0, since the existing assignment is [i8, i1], the next available instance from the candidates is i12.
    //   For p1, the existing assignment is [i4, i8], the next available instance is also i12.
    // r0: [i12, i4, i8, i1]
    // For r1, the candidate instances become [i10, i13, i5, i7, i9].
    //   For p0, since the existing assignment is [i9, i10], the next available instance is i13 (new instance).
    //   For p1, the existing assignment is [i5, i9], the next available one from the candidates is i10, but since
    //   i10 is already used in the former partition, it got added to the tail, so the next available one is i13.
    // r1: [i10, i13, i5, i9]
    // For r2, the candidate instances become [i11, i3, i7, i0].
    //   For p0, the existing assignment is [i0, i3], the next available instance from the candidates is i11.
    //   For p1, the existing assignment is [i11, i0], the next available instance from the candidates is i3, but
    //   since i3 is already used in the former partition, it got appended to the tail, so the next available one is i7.
    // r2: [i11, i3, i7, i0]
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 12));
    assertEquals(instancePartitions.getInstances(1, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 12));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 10, SERVER_INSTANCE_ID_PREFIX + 13));
    assertEquals(instancePartitions.getInstances(1, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 13));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 11));
    assertEquals(instancePartitions.getInstances(1, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 11, SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 7));

    // Reduce the number of instances per replica group from 3 to 2.
    numInstancesPerPartition = 2;
    tableConfig.getValidationConfig()
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(partitionColumnName, numInstancesPerPartition));

    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, instancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicas);
    assertEquals(instancePartitions.getNumPartitions(), numPartitions);

    // The instance assignment should be the same as the one without the newly added instances.
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(1, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 8));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 10));
    assertEquals(instancePartitions.getInstances(1, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(1, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 11, SERVER_INSTANCE_ID_PREFIX + 0));

    // Add one more replica group (from 3 to 4).
    numReplicas = 4;
    tableConfig.getValidationConfig().setReplication(Integer.toString(numReplicas));
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, instancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicas);
    assertEquals(instancePartitions.getNumPartitions(), numPartitions);

    // Math.abs("myTable_OFFLINE".hashCode()) % 12 = 2
    // [i10, i11, i12, i13, i3, i4, i5, i11, i7, i8, i9, i0, i1]
    // The existing replica groups remain unchanged.
    // For the new replica group r3, the candidate instances become [i12, i13, i7].
    // r3: [i12, i13, i7]
    //       p0, p0, p1
    //       p1
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(1, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 8));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 10));
    assertEquals(instancePartitions.getInstances(1, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(1, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 11, SERVER_INSTANCE_ID_PREFIX + 0));
    assertEquals(instancePartitions.getInstances(0, 3),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 12, SERVER_INSTANCE_ID_PREFIX + 13));
    assertEquals(instancePartitions.getInstances(1, 3),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 7, SERVER_INSTANCE_ID_PREFIX + 12));

    // Remove one replica group (from 4 to 3).
    numReplicas = 3;
    tableConfig.getValidationConfig().setReplication(Integer.toString(numReplicas));
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, instancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicas);
    assertEquals(instancePartitions.getNumPartitions(), numPartitions);

    // The output should be the same as the one before adding one replica group.
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(1, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 8));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 10));
    assertEquals(instancePartitions.getInstances(1, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(1, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 11, SERVER_INSTANCE_ID_PREFIX + 0));
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
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, 0, 0, 0, false, null);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
            new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig))).build();
    InstanceAssignmentDriver driver = new InstanceAssignmentDriver(tableConfig);

    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // All instances in pool 0 should be assigned to replica-group 0, and all instances in pool 1 should be assigned to
    // replica-group 1
    // Math.abs("myTable_OFFLINE".hashCode()) % 5 = 3
    // pool 0: [ i3, i4, i0, i1, i2 ]
    // pool 1: [ i8, i9, i5, i6, i7 ]
    InstancePartitions instancePartitions =
        driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 5,
            SERVER_INSTANCE_ID_PREFIX + 6, SERVER_INSTANCE_ID_PREFIX + 7));

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
    // Math.abs("myTable_OFFLINE".hashCode()) % 5 = 3
    // pool 0: [  i3,  i4,  i0,  i1,  i2 ]
    // pool 2: [ i13, i14, i10, i11, i12 ]
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 13, SERVER_INSTANCE_ID_PREFIX + 14, SERVER_INSTANCE_ID_PREFIX + 10,
            SERVER_INSTANCE_ID_PREFIX + 11, SERVER_INSTANCE_ID_PREFIX + 12));

    // Select all 3 pools in pool selection
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig)));

    // Math.abs("myTable_OFFLINE".hashCode()) % 3 = 2
    // All instances in pool 2 should be assigned to replica-group 0, and all instances in pool 0 should be assigned to
    // replica-group 1
    // Math.abs("myTable_OFFLINE".hashCode()) % 5 = 3
    // The instances should be rotated 3 places
    // pool 2: [ i13, i14, i10, i11, i12 ]
    // pool 0: [  i3,  i4,  i0,  i1,  i2 ]
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 13, SERVER_INSTANCE_ID_PREFIX + 14, SERVER_INSTANCE_ID_PREFIX + 10,
            SERVER_INSTANCE_ID_PREFIX + 11, SERVER_INSTANCE_ID_PREFIX + 12));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 2));

    // Select pool 0 and 1 in pool selection
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, 0, Arrays.asList(0, 1));
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig)));

    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // All instances in pool 0 should be assigned to replica-group 0, and all instances in pool 1 should be assigned to
    // replica-group 1
    // Math.abs("myTable_OFFLINE".hashCode()) % 5 = 3
    // pool 0: [ i3, i4, i0, i1, i2 ]
    // pool 1: [ i8, i9, i5, i6, i7 ]
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 5,
            SERVER_INSTANCE_ID_PREFIX + 6, SERVER_INSTANCE_ID_PREFIX + 7));

    // Assign instances from 2 pools to 3 replica-groups
    numReplicaGroups = numPools;
    replicaPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, 0, 0, 0, false, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
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
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 0));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 1));

    // ===== Test against the cases when the existing instancePartitions isn't null,
    //       and minimizeDataMovement is set to true. =====
    // Reset the number of replica groups to 2 and pools to 2.
    numReplicaGroups = 2;
    numPools = 2;
    replicaPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, 0, 0, 0, true, null);
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig)));
    // Reset the instance configs to have only two pools.
    instanceConfigs.clear();
    numInstances = 10;
    for (int i = 0; i < numInstances; i++) {
      InstanceConfig instanceConfig = new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i);
      instanceConfig.addTag(OFFLINE_TAG);
      int pool = i / numInstancesPerPool;
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }

    // Use all pools, the instancePartitions should be the same as the one without using
    // the existing partition to instances map.
    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // All instances in pool 0 should be assigned to replica-group 0, and all instances in pool 1 should be assigned to
    // replica-group 1.
    // [pool0, pool1]
    //  r0     r1
    InstancePartitions existingInstancePartitions = null;
    instancePartitions =
        driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, existingInstancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 5,
            SERVER_INSTANCE_ID_PREFIX + 6, SERVER_INSTANCE_ID_PREFIX + 7));

    // Get the latest existingPoolToInstancesMap from last computation and try again.
    // The actual assignment should be the same as last one.
    existingInstancePartitions = instancePartitions;
    instancePartitions =
        driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, existingInstancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 5,
            SERVER_INSTANCE_ID_PREFIX + 6, SERVER_INSTANCE_ID_PREFIX + 7));

    // Select pool 0 and 1 in pool selection
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, 0, Arrays.asList(0, 1));
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig)));

    // Get the latest existingInstancePartitions from last computation.
    existingInstancePartitions = instancePartitions;

    // Putting the existingPoolToInstancesMap shouldn't change the instance assignment.
    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // All instances in pool 0 should be assigned to replica-group 0, and all instances in pool 1 should be assigned to
    // replica-group 1
    // Now in poolToInstancesMap:
    // pool 0: [ i3, i4, i0, i1, i2 ]
    // pool 1: [ i8, i9, i5, i6, i7 ]
    instancePartitions =
        driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, existingInstancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 5,
            SERVER_INSTANCE_ID_PREFIX + 6, SERVER_INSTANCE_ID_PREFIX + 7));

    // Assign instances from 2 pools to 3 replica-groups
    numReplicaGroups = 3;
    replicaPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, 0, 0, 0, true, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig)));

    // Get the latest existingInstancePartitions from last computation.
    existingInstancePartitions = instancePartitions;

    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // [pool0, pool1]
    //  r0     r1
    //  r2
    // Each replica-group should have 2 instances assigned
    // Math.abs("myTable_OFFLINE".hashCode()) % 5 = 3
    // Latest instances from ZK:
    //   pool 0: [ i3, i4, i0, i1, i2 ]
    //   pool 1: [ i8, i9, i5, i6, i7 ]
    // i3 and i4 will be retained for r0, i8 and i9 will be retained for r1. i0 and i1 are picked up from the latest
    // instances in the target pool.
    // Thus, the new assignment will be as follows:
    //   pool 0: [ i3, i4, i0, i1, i2 ]
    //             r0  r0  r2  r2
    //   pool 1: [ i8, i9, i5, i6, i7 ]
    //             r1  r1
    instancePartitions =
        driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, existingInstancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 1));

    // Remove one instance from each of the pools and add one more back.
    instanceConfigs.remove(8);
    instanceConfigs.remove(3);
    int poolCount = 0;
    for (int i = numInstances; i < numInstances + 2; i++) {
      InstanceConfig instanceConfig = new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i);
      instanceConfig.addTag(OFFLINE_TAG);
      int pool = poolCount++;
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }

    // Get the latest existingInstancePartitions from last computation.
    existingInstancePartitions = instancePartitions;

    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // [pool0, pool1]
    //  r0     r1
    //  r2
    // Each replica-group should have 2 instances assigned
    // Math.abs("myTable_OFFLINE".hashCode()) % 5 = 3
    // Latest instances from ZK:
    //     pool 0: [ i2, i4,  i0, i1, i10 ]
    //     pool 1: [ i7, i9, i11, i5,  i6 ]
    // i3 gets swapped out, the next available instance i2 will take its place.
    // Similarly, i8 is swapped out and i7 will take its place.
    // Thus, the new assignment will be as follows:
    //     pool 0: [ i2, i4,  i0, i1, i10 ]
    //               r0  r0   r2  r2
    //     pool 1: [ i7, i9, i11, i5,  i6 ]
    //               r1  r1
    instancePartitions =
        driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, existingInstancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 2, SERVER_INSTANCE_ID_PREFIX + 4));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 7, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 1));

    // Reduce number of replica groups from 3 to 2.
    numReplicaGroups = 2;
    replicaPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, 0, 0, 0, true, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig)));

    // Get the latest existingInstancePartitions from last computation.
    existingInstancePartitions = instancePartitions;

    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // [pool0, pool1]
    //  r0     r1
    // Each replica-group should have 2 instances assigned
    // Math.abs("myTable_OFFLINE".hashCode()) % 5 = 3
    // Latest instances from ZK:
    //     pool 0: [ i2, i4,  i0, i1, i10 ]
    //     pool 1: [ i7, i9, i11, i5,  i6 ]
    // In the existing instancePartitions, r0 already has [i2, i4], append the rest available instances
    // (ie. [i0, i1, i10]) to the tail.
    // r1 already has [i7, i9], append the rest available instances (ie. [i11, i5, i6]) to the tail.
    // Thus, the new assignment will become:
    //     pool 0: [ i2, i4,  i0, i1, i10 ]
    //               r0  r0   r0  r0   r0
    //     pool 1: [ i7, i9, i11, i5,  i6 ]
    //               r1  r1   r1  r1   r1
    instancePartitions =
        driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, existingInstancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 2, SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 10));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 7, SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 11,
            SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 6));

    // Add 1 more instances to each pool
    poolCount = 0;
    for (int i = numInstances + 2; i < numInstances + 4; i++) {
      InstanceConfig instanceConfig = new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i);
      instanceConfig.addTag(OFFLINE_TAG);
      int pool = poolCount++;
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }

    // Get the latest existingInstancePartitions from last computation.
    existingInstancePartitions = instancePartitions;

    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // [pool0, pool1]
    //  r0     r1
    // Each replica-group should have 2 instances assigned
    // Math.abs("myTable_OFFLINE".hashCode()) % 6 = 2
    // Latest instances from ZK:
    //     pool 0: [ i10, i12, i2, i4,  i0,  i1 ]
    //     pool 1: [  i5,  i6, i7, i9, i11, i13 ]
    // There is one more empty position for each of the replica groups.
    // Append the newly added instances (i.e. i12 and i13) to the tails.
    // Thus, the new assignment will become:
    //     pool 0: [ i2, i4,  i0, i1, i10, i12 ]
    //     pool 1: [ i7, i9, i11, i5,  i6, i13 ]
    instancePartitions =
        driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, existingInstancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 2, SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 10, SERVER_INSTANCE_ID_PREFIX + 12));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 7, SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 11,
            SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 6, SERVER_INSTANCE_ID_PREFIX + 13));

    // Remove one instances from each of the pools, i.e. i2 and i5.
    instanceConfigs.remove(4);
    instanceConfigs.remove(2);

    // Get the latest existingInstancePartitions from last computation.
    existingInstancePartitions = instancePartitions;

    // Math.abs("myTable_OFFLINE".hashCode()) % 2 = 0
    // [pool0, pool1]
    //  r0     r1
    // Each replica-group should have 2 instances assigned
    // Math.abs("myTable_OFFLINE".hashCode()) % 5 = 3
    // Latest instances from ZK:
    //     pool 0: [ i12, i4,  i0, i1, i10 ]
    //     pool 1: [ i7,  i9, i11, i13, i6 ]
    // Since i2 and i5 got removed from the pools, the tail instances (i.e. i12 and 13) will be used to fill their
    // vacant position.
    // Thus, the new assignment will become:
    //     pool 0: [ i12, i4,  i0,  i1, i10 ]
    //     pool 1: [  i7, i9, i11, i13,  i6 ]
    instancePartitions =
        driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, existingInstancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 12, SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1, SERVER_INSTANCE_ID_PREFIX + 10));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 7, SERVER_INSTANCE_ID_PREFIX + 9, SERVER_INSTANCE_ID_PREFIX + 11,
            SERVER_INSTANCE_ID_PREFIX + 13, SERVER_INSTANCE_ID_PREFIX + 6));
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
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Instance assignment is not allowed for the given table config");
    }

    InstanceTagPoolConfig tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, false, 0, null);
    InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(false, 0, 0, 0, 0, 0, false, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // No instance with correct tag
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "No enabled instance has the tag: tenant_OFFLINE");
    }

    for (InstanceConfig instanceConfig : instanceConfigs) {
      instanceConfig.addTag(OFFLINE_TAG);
    }

    // All instances should be assigned as replica-group 0 partition 0
    InstancePartitions instancePartitions =
        driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), 1);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    List<String> expectedInstances = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      expectedInstances.add(SERVER_INSTANCE_ID_PREFIX + i);
    }
    // Math.abs("myTable_OFFLINE".hashCode()) % 10 = 8
    Collections.rotate(expectedInstances, -8);
    assertEquals(instancePartitions.getInstances(0, 0), expectedInstances);

    // Enable pool
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, 0, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // No instance has correct pool configured
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
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
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), 1);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    expectedInstances.clear();
    for (int i = 0; i < numInstances / 2; i++) {
      expectedInstances.add(SERVER_INSTANCE_ID_PREFIX + i);
    }
    // Math.abs("myTable_OFFLINE".hashCode()) % 5 = 3
    Collections.rotate(expectedInstances, -3);
    assertEquals(instancePartitions.getInstances(0, 0), expectedInstances);

    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, 3, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Ask for too many pools
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Not enough instance pools (2 in the cluster, asked for 3)");
    }

    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, 0, Arrays.asList(0, 2));
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Ask for pool that does not exist
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Cannot find all instance pools configured: [0, 2]");
    }

    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, 0, null);
    replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(false, 6, 0, 0, 0, 0, false, null
    );
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Ask for too many instances
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Not enough qualified instances from pool: 0 (5 in the pool, asked for 6)");
    }

    // Enable replica-group
    replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, 0, 0, 0, 0, false, null
    );
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Number of replica-groups must be positive
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Number of replica-groups must be positive");
    }

    replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, 11, 0, 0, 0, false, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Ask for too many replica-groups
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Not enough qualified instances from pool: 0, cannot select 6 replica-groups from 5 instances");
    }

    replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, 3, 3, 0, 0, false, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Ask for too many instances
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Not enough qualified instances from pool: 0 (5 in the pool, asked for 6)");
    }

    replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, 3, 2, 0, 3, false, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Ask for too many instances per partition
    try {
      driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Number of instances per partition: 3 must be smaller or equal to number of instances per replica-group: 2");
    }

    replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, 3, 2, 0, 0, false, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig)));

    // Math.abs("myTable_OFFLINE".hashCode()) % 5 = 3
    // pool0: [i3, i4, i0, i1, i2]
    //         r0  r2  r0  r2
    // pool1: [i8, i9, i5, i6, i7]
    //         r1  r1
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), 3);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 0));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 8, SERVER_INSTANCE_ID_PREFIX + 9));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 1));

    // Illegal partition selector
    numInstances = 21;
    int numPools = 5;
    int numReplicaGroups = 3;
    int numInstancesPerReplicaGroup = numInstances / numReplicaGroups;
    instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    InstanceReplicaGroupPartitionConfig replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, numInstancesPerReplicaGroup, 0,
            0, false, null);

    try {
      tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
          .setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
              new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig, "ILLEGAL_SELECTOR"))).build();
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(),
          "No enum constant org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig.PartitionSelector"
              + ".ILLEGAL_SELECTOR");
    }

    // The total num instances cannot be assigned evenly to replica groups
    numInstances = 21;
    numPools = 5;
    numReplicaGroups = 4;
    numInstancesPerReplicaGroup = 0;
    instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, numInstancesPerReplicaGroup,
            0, 0, false, null);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setInstanceAssignmentConfigMap(
        Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
            new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig,
                InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.toString()))).build();
    driver = new InstanceAssignmentDriver(tableConfig);
    try {
      instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "The total num instances 21 cannot be assigned evenly to 4 replica groups, please specify a "
              + "numInstancesPerReplicaGroup in _replicaGroupPartitionConfig");
    }

    // The total num instances are not enough
    numInstances = 21;
    numPools = 5;
    numReplicaGroups = 4;
    numInstancesPerReplicaGroup = 6;
    instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups,
            numInstancesPerReplicaGroup, 0, 0, false, null);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setInstanceAssignmentConfigMap(
        Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
            new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig,
                InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.toString()))).build();
    driver = new InstanceAssignmentDriver(tableConfig);
    try {
      instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Not enough qualified instances, ask for: (numInstancesPerReplicaGroup: 6) * "
          + "(numReplicaGroups: 4) = 24, having only 21");
    }

    numInstances = 10;
    numPools = 5;
    numReplicaGroups = 5;
    numInstancesPerReplicaGroup = numInstances / numReplicaGroups;
    instanceConfigs = new LinkedList<>();
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }

    // The instances are not balanced for each pool (fault-domain)
    instanceConfigs.remove(9);
    InstanceConfig instanceConfig =
        new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + 10 + SERVER_INSTANCE_POOL_PREFIX + 0);
    instanceConfig.addTag(OFFLINE_TAG);
    instanceConfig.getRecord()
        .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(0)));
    instanceConfigs.add(instanceConfig);

    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups,
            numInstancesPerReplicaGroup, 0, 0, false, null);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setInstanceAssignmentConfigMap(
        Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
            new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig,
                InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.toString()))).build();
    driver = new InstanceAssignmentDriver(tableConfig);
    try {
      instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "The instances are not balanced for each pool (fault-domain)");
    }
  }

  @Test
  public void testPoolBasedFDAware() {
    // 21 instances in 5 pools, with [5,4,4,4,4] instances in each pool
    int numPartitions = 0;
    int numInstancesPerPartition = 0;
    int numInstances = 21;
    int numPools = 5;
    int numReplicaGroups = 3;
    int numInstancesPerReplicaGroup = numInstances / numReplicaGroups;
    List<InstanceConfig> instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }
    // Use all pools
    InstanceTagPoolConfig tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    // Assign to 3 replica-groups so that each replica-group is assigned 7 instances
    InstanceReplicaGroupPartitionConfig replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, numInstancesPerReplicaGroup, numPartitions,
            numInstancesPerPartition, false, null);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
            new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig,
                InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.toString()))).build();
    InstanceAssignmentDriver driver = new InstanceAssignmentDriver(tableConfig);

    InstancePartitions instancePartitions =
        driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    /*
     * 21 instances in 5 FDs (pools), with [5,4,4,4,4] instances in each FD
     * (tableNameHash % poolToInstanceEntriesList.size()) = 3
     *
     *         RG1(FD)    RG2(FD)    RG3(FD)
     *   Host  20 (0)     7  (2)     14 (4)
     *   Host  1  (1)     8  (3)     10 (0)
     *   Host  2  (2)     9  (4)     16 (1)
     *   Host  3  (3)     0  (0)     17 (2)
     *   Host  4  (4)     11 (1)     18 (3)
     *   Host  5  (0)     12 (2)     19 (4)
     *   Host  6  (1)     13 (3)     15 (0)
     */
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 20 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 2 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 3 + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + 4 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 5 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 6 + SERVER_INSTANCE_POOL_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 7 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 8 + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + 9 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 0 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 11 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 12 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 13 + SERVER_INSTANCE_POOL_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 14 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 10 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 16 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 17 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 18 + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + 19 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 15 + SERVER_INSTANCE_POOL_PREFIX + 0));

    // 28 instances in 5 pools, with [6,6,6,5,5] instances in each pool
    // minimized movement on top of the above assignment
    numPartitions = 0;
    numInstancesPerPartition = 0;
    numInstances = 28;
    numPools = 5;
    numReplicaGroups = 4;
    numInstancesPerReplicaGroup = numInstances / numReplicaGroups;
    instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }
    // Use all pools
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    // Assign to 3 replica-groups so that each replica-group is assigned 7 instances
    replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, numInstancesPerReplicaGroup, numPartitions,
            numInstancesPerPartition, true, null);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setInstanceAssignmentConfigMap(
        Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
            new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig,
                InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.toString()))).build();
    driver = new InstanceAssignmentDriver(tableConfig);
    // existingInstancePartitions = instancePartitions
    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, instancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    /*
     * 28 instances in 5 FDs (pools), with [6,6,6,5,5] instances in each FD
     * incremental assignment based on the previous assignment of 21 instances in 3RGs
     *
     *         RG1(FD)    RG2(FD)    RG3(FD)    RG4(FD)
     *   Host  20 (0)     7  (2)     14 (4)     21 (1)
     *   Host  1  (1)     8  (3)     10 (0)     22 (2)
     *   Host  2  (2)     9  (4)     16 (1)     23 (3)
     *   Host  3  (3)     0  (0)     17 (2)     24 (4)
     *   Host  4  (4)     11 (1)     18 (3)     25 (0)
     *   Host  5  (0)     12 (2)     19 (4)     26 (1)
     *   Host  6  (1)     13 (3)     15 (0)     27 (2)
     */
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 20 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 2 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 3 + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + 4 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 5 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 6 + SERVER_INSTANCE_POOL_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 7 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 8 + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + 9 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 0 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 11 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 12 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 13 + SERVER_INSTANCE_POOL_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 14 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 10 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 16 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 17 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 18 + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + 19 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 15 + SERVER_INSTANCE_POOL_PREFIX + 0));
    assertEquals(instancePartitions.getInstances(0, 3),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 21 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 22 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 23 + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + 24 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 25 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 26 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 27 + SERVER_INSTANCE_POOL_PREFIX + 2));

    // 21 instances in 5 pools, with [5,4,4,4,4] instances in each pool
    // Partitioned at table level (only segments are partitioned, RG not aware of the partitioning)
    int numPartitionsSegment = 3;
    numInstances = 21;
    numPools = 5;
    numReplicaGroups = 3;
    numInstancesPerReplicaGroup = numInstances / numReplicaGroups;
    instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }
    // Use all pools
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    // Assign to 3 replica-groups so that each replica-group is assigned 7 instances
    replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, numInstancesPerReplicaGroup, numPartitions,
            numInstancesPerPartition, false, null);
    String partitionColumnName = "partition";
    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(
        Collections.singletonMap(partitionColumnName, new ColumnPartitionConfig("Modulo", numPartitionsSegment, null)));
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setInstanceAssignmentConfigMap(
            Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
                new InstanceAssignmentConfig(tagPoolConfig, null, replicaPartitionConfig,
                    InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.toString())))
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(partitionColumnName, numInstancesPerReplicaGroup))
        .setSegmentPartitionConfig(segmentPartitionConfig).build();
    driver = new InstanceAssignmentDriver(tableConfig);

    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    /*
     * 21 instances in 5 FDs (pools), with [5,4,4,4,4] instances in each FD
     * (tableNameHash % poolToInstanceEntriesList.size()) = 3
     *
     *         RG1(FD)    RG2(FD)    RG3(FD)
     *   Host  20 (0)     7  (2)     14 (4)
     *   Host  1  (1)     8  (3)     10 (0)
     *   Host  2  (2)     9  (4)     16 (1)
     *   Host  3  (3)     0  (0)     17 (2)
     *   Host  4  (4)     11 (1)     18 (3)
     *   Host  5  (0)     12 (2)     19 (4)
     *   Host  6  (1)     13 (3)     15 (0)
     */
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 20 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 2 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 3 + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + 4 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 5 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 6 + SERVER_INSTANCE_POOL_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 7 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 8 + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + 9 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 0 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 11 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 12 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 13 + SERVER_INSTANCE_POOL_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 14 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 10 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 16 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 17 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 18 + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + 19 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 15 + SERVER_INSTANCE_POOL_PREFIX + 0));

    // 9 instances in 5 pools, with [2,2,2,2,1] instances in each pool
    numInstances = 9;
    numPools = 5;
    numReplicaGroups = 3;
    numInstancesPerReplicaGroup = numInstances / numReplicaGroups;
    instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }
    // Use all pools
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    // Assign to 3 replica-groups so that each replica-group is assigned 3 instances
    replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, numInstancesPerReplicaGroup, numPartitions,
            numInstancesPerPartition, false, null);
    // Do not rotate for testing
    InstanceConstraintConfig instanceConstraintConfig =
        new InstanceConstraintConfig(Arrays.asList("constraint1", "constraint2"));
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME + TABLE_NAME_ZERO_HASH_COMPLEMENT)
            .setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
                new InstanceAssignmentConfig(tagPoolConfig, instanceConstraintConfig, replicaPartitionConfig,
                    InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.toString())))
            .build();
    driver = new InstanceAssignmentDriver(tableConfig);

    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    /*
     * 15 instances in 5 FDs (pools), with [2,2,2,2,1] instances in each FD
     *
     *         RG1(FD)   RG2(FD)   RG3(FD)
     *   Host  0  (0)    3  (3)    6  (1)
     *   Host  1  (1)    4  (4)    7  (2)
     *   Host  2  (2)    5  (0)    8  (3)
     *
     */
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 2 + SERVER_INSTANCE_POOL_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 3 + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + 4 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 5 + SERVER_INSTANCE_POOL_PREFIX + 0));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 6 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 7 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 8 + SERVER_INSTANCE_POOL_PREFIX + 3));

    // 9 instances in 5 pools, with [2,2,2,2,1] instances in each pool
    numInstances = 16;
    numPools = 5;
    numReplicaGroups = 4;
    numInstancesPerReplicaGroup = numInstances / numReplicaGroups;
    instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }
    // Use all pools
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    // Assign to 3 replica-groups so that each replica-group is assigned 3 instances
    replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, numInstancesPerReplicaGroup, numPartitions,
            numInstancesPerPartition, true, null);
    // Do not rotate for testing
    instanceConstraintConfig = new InstanceConstraintConfig(Arrays.asList("constraint1", "constraint2"));
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME + TABLE_NAME_ZERO_HASH_COMPLEMENT)
            .setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
                new InstanceAssignmentConfig(tagPoolConfig, instanceConstraintConfig, replicaPartitionConfig,
                    InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.toString())))
            .build();
    driver = new InstanceAssignmentDriver(tableConfig);

    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, instancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    /*
     * 15 instances in 5 FDs (pools), with [2,2,2,2,1] instances in each FD
     *
     *         RG1(FD)   RG2(FD)   RG3(FD)   RG4(FD)
     *   Host  0  (0)    3  (3)    6  (1)    14 (4)
     *   Host  1  (1)    4  (4)    7  (2)    15 (0)
     *   Host  2  (2)    5  (0)    8  (3)    9  (4)
     *   Host  10 (0)    11 (1)    12 (2)    13 (3)
     */
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 1 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 2 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 10 + SERVER_INSTANCE_POOL_PREFIX + 0));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 3 + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + 4 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 5 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 11 + SERVER_INSTANCE_POOL_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 6 + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + 7 + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + 8 + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + 12 + SERVER_INSTANCE_POOL_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(0, 3),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 14 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 15 + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + 9 + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 13 + SERVER_INSTANCE_POOL_PREFIX + 3));

    // 15 instances in 5 pools, with [3,3,3,3,3] instances in each pool
    numPartitions = 0;
    numInstancesPerPartition = 0;
    numInstances = 15;
    numPools = 5;
    numReplicaGroups = 3;
    numInstancesPerReplicaGroup = numInstances / numReplicaGroups;
    instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + String.format("%02d", i) + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }
    // Use all pools
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    // Assign to 3 replica-groups so that each replica-group is assigned 5 instances
    replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, numInstancesPerReplicaGroup, numPartitions,
            numInstancesPerPartition, false, null);
    // Do not rotate instance sequence in pool (for testing)
    instanceConstraintConfig = new InstanceConstraintConfig(Arrays.asList("constraint1", "constraint2"));
    // Do not rotate pool sequence (for testing)
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME + TABLE_NAME_ZERO_HASH_COMPLEMENT)
            .setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
                new InstanceAssignmentConfig(tagPoolConfig, instanceConstraintConfig, replicaPartitionConfig,
                    InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.toString())))
            .build();
    driver = new InstanceAssignmentDriver(tableConfig);

    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    /*
     * 15 instances in 5 FDs (pools), with [3,3,3,3,3] instances in each FD
     *
     *         RG1(FD)   RG2(FD)   RG3(FD)
     *   Host  0  (0)    9  (4)    13  (3)
     *   Host  1  (1)    5  (0)    14  (4)
     *   Host  2  (2)    6  (1)    10  (0)
     *   Host  3  (3)    7  (2)    11  (1)
     *   Host  4  (4)    8  (3)    12  (2)
     */
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "00" + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + "01" + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + "02" + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + "03" + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + "04" + SERVER_INSTANCE_POOL_PREFIX + 4));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "09" + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + "05" + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + "06" + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + "07" + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + "08" + SERVER_INSTANCE_POOL_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "13" + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + "14" + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + "10" + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + "11" + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + "12" + SERVER_INSTANCE_POOL_PREFIX + 2));

    // 15 instances in 5 pools, with [3,3,3,3,3] instances in each pool
    numPartitions = 0;
    numInstancesPerPartition = 0;
    numInstances = 20;
    numPools = 5;
    numReplicaGroups = 4;
    numInstancesPerReplicaGroup = numInstances / numReplicaGroups;
    instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + String.format("%02d", i) + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }
    // Use all pools
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    // Assign to 3 replica-groups so that each replica-group is assigned 5 instances
    replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, numInstancesPerReplicaGroup, numPartitions,
            numInstancesPerPartition, true, null);
    // Do not rotate instance sequence in pool (for testing)
    instanceConstraintConfig = new InstanceConstraintConfig(Arrays.asList("constraint1", "constraint2"));
    // Do not rotate pool sequence (for testing)
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME + TABLE_NAME_ZERO_HASH_COMPLEMENT)
            .setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
                new InstanceAssignmentConfig(tagPoolConfig, instanceConstraintConfig, replicaPartitionConfig,
                    InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.toString())))
            .build();
    driver = new InstanceAssignmentDriver(tableConfig);

    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, instancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    /*
     * 20 instances in 5 FDs (pools), with [4,4,4,4,4] instances in each FD
     *
     *         RG1(FD)   RG2(FD)   RG3(FD)   RG4(FD)
     *   Host  0  (0)    9  (4)    13  (3)   17  (2)
     *   Host  1  (1)    5  (0)    14  (4)   18  (3)
     *   Host  2  (2)    6  (1)    10  (0)   19  (4)
     *   Host  3  (3)    7  (2)    11  (1)   15  (0)
     *   Host  4  (4)    8  (3)    12  (2)   16  (1)
     */
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "00" + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + "01" + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + "02" + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + "03" + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + "04" + SERVER_INSTANCE_POOL_PREFIX + 4));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "09" + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + "05" + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + "06" + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + "07" + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + "08" + SERVER_INSTANCE_POOL_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "13" + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + "14" + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + "10" + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + "11" + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + "12" + SERVER_INSTANCE_POOL_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(0, 3),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "17" + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + "18" + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + "19" + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + "15" + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + "16" + SERVER_INSTANCE_POOL_PREFIX + 1));

    // 3 instances in 5 pools, with [1,1,1,0,0] instances in each pool
    numPartitions = 0;
    numInstancesPerPartition = 0;
    numInstances = 3;
    numPools = 3;
    numReplicaGroups = 3;
    numInstancesPerReplicaGroup = numInstances / numReplicaGroups;
    instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + i + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }
    // Use all pools
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    // Assign to 3 replica-groups so that each replica-group is assigned 1 instances
    replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, numInstancesPerReplicaGroup, numPartitions,
            numInstancesPerPartition, false, null);
    // Do not rotate for testing
    instanceConstraintConfig = new InstanceConstraintConfig(Arrays.asList("constraint1", "constraint2"));
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME + TABLE_NAME_ZERO_HASH_COMPLEMENT)
            .setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
                new InstanceAssignmentConfig(tagPoolConfig, instanceConstraintConfig, replicaPartitionConfig,
                    InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.toString())))
            .build();
    driver = new InstanceAssignmentDriver(tableConfig);

    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    /*
     * 3 instances in 5 FDs (pools), with [1,1,1,0,0] instances in each FD
     *
     *         RG1(FD)   RG2(FD)   RG3(FD)
     *   Host  0  (0)    1  (1)    2  (2)
     *
     */
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 0 + SERVER_INSTANCE_POOL_PREFIX + 0));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 1 + SERVER_INSTANCE_POOL_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 2 + SERVER_INSTANCE_POOL_PREFIX + 2));

    // 12 instances in 5 pools, with [3,3,2,2,2] instances in each pool
    numPartitions = 0;
    numInstancesPerPartition = 0;
    numInstances = 12;
    numPools = 5;
    numReplicaGroups = 6;
    numInstancesPerReplicaGroup = numInstances / numReplicaGroups;
    instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + String.format("%02d", i) + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }
    // Use all pools
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    // Assign to 6 replica-groups so that each replica-group is assigned 2 instances
    replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, numInstancesPerReplicaGroup, numPartitions,
            numInstancesPerPartition, false, null);
    // Do not rotate instance sequence in pool (for testing)
    instanceConstraintConfig = new InstanceConstraintConfig(Arrays.asList("constraint1", "constraint2"));
    // Do not rotate pool sequence (for testing)
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME + TABLE_NAME_ZERO_HASH_COMPLEMENT)
            .setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
                new InstanceAssignmentConfig(tagPoolConfig, instanceConstraintConfig, replicaPartitionConfig,
                    InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.toString())))
            .build();
    driver = new InstanceAssignmentDriver(tableConfig);

    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, null);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    /*
     * 12 instances in 5 FDs (pools), with [3,3,2,2,2] instances in each FD
     *
     *         RG0(FD)   RG1(FD)   RG2(FD)   RG3(FD)   RG4(FD)    RG5(FD)
     *   Host  0  (0)    2 (2)     4  (4)    6  (1)    8  (3)     10  (0)
     *   Host  1  (1)    3 (3)     5  (0)    7  (2)    9  (4)     11  (1)
     */
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "00" + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + "01" + SERVER_INSTANCE_POOL_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "02" + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + "03" + SERVER_INSTANCE_POOL_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "04" + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + "05" + SERVER_INSTANCE_POOL_PREFIX + 0));
    assertEquals(instancePartitions.getInstances(0, 3),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "06" + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + "07" + SERVER_INSTANCE_POOL_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(0, 4),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "08" + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + "09" + SERVER_INSTANCE_POOL_PREFIX + 4));
    assertEquals(instancePartitions.getInstances(0, 5),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "10" + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + "11" + SERVER_INSTANCE_POOL_PREFIX + 1));

    // 18 instances in 5 pools, with [4,4,4,3,3] instances in each pool
    // increment 1 instance per replica group on top of the above assignment
    numPartitions = 0;
    numInstancesPerPartition = 0;
    numInstances = 18;
    numPools = 5;
    numReplicaGroups = 6;
    numInstancesPerReplicaGroup = numInstances / numReplicaGroups;
    instanceConfigs = new ArrayList<>(numInstances);
    for (int i = 0; i < numInstances; i++) {
      int pool = i % numPools;
      InstanceConfig instanceConfig =
          new InstanceConfig(SERVER_INSTANCE_ID_PREFIX + String.format("%02d", i) + SERVER_INSTANCE_POOL_PREFIX + pool);
      instanceConfig.addTag(OFFLINE_TAG);
      instanceConfig.getRecord()
          .setMapField(InstanceUtils.POOL_KEY, Collections.singletonMap(OFFLINE_TAG, Integer.toString(pool)));
      instanceConfigs.add(instanceConfig);
    }
    // Use all pools
    tagPoolConfig = new InstanceTagPoolConfig(OFFLINE_TAG, true, numPools, null);
    // Assign to 6 replica-groups so that each replica-group is assigned 2 instances
    replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, numInstancesPerReplicaGroup, numPartitions,
            numInstancesPerPartition, true, null);
    // Do not rotate instance sequence in pool (for testing)
    instanceConstraintConfig = new InstanceConstraintConfig(Arrays.asList("constraint1", "constraint2"));
    // Do not rotate pool sequence (for testing)
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME + TABLE_NAME_ZERO_HASH_COMPLEMENT)
            .setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
                new InstanceAssignmentConfig(tagPoolConfig, instanceConstraintConfig, replicaPartitionConfig,
                    InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.toString())))
            .build();
    driver = new InstanceAssignmentDriver(tableConfig);

    instancePartitions = driver.assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs, instancePartitions);
    assertEquals(instancePartitions.getNumReplicaGroups(), numReplicaGroups);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    /*
     * 18 instances in 5 FDs (pools), with [4,4,4,3,3] instances in each FD
     *
     *         RG0(FD)   RG1(FD)   RG2(FD)   RG3(FD)   RG4(FD)    RG5(FD)
     *   Host  0  (0)    2 (2)     4  (4)    6  (1)    8  (3)     10  (0)
     *   Host  1  (1)    3 (3)     5  (0)    7  (2)    9  (4)     11  (1)
     *   Host  15 (0)    16(1)     12 (2)    13 (3)    14 (4)     17  (2)
     *
     */
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "00" + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + "01" + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + "15" + SERVER_INSTANCE_POOL_PREFIX + 0));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "02" + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + "03" + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + "16" + SERVER_INSTANCE_POOL_PREFIX + 1));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "04" + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + "05" + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + "12" + SERVER_INSTANCE_POOL_PREFIX + 2));
    assertEquals(instancePartitions.getInstances(0, 3),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "06" + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + "07" + SERVER_INSTANCE_POOL_PREFIX + 2,
            SERVER_INSTANCE_ID_PREFIX + "13" + SERVER_INSTANCE_POOL_PREFIX + 3));
    assertEquals(instancePartitions.getInstances(0, 4),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "08" + SERVER_INSTANCE_POOL_PREFIX + 3,
            SERVER_INSTANCE_ID_PREFIX + "09" + SERVER_INSTANCE_POOL_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + "14" + SERVER_INSTANCE_POOL_PREFIX + 4));
    assertEquals(instancePartitions.getInstances(0, 5),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + "10" + SERVER_INSTANCE_POOL_PREFIX + 0,
            SERVER_INSTANCE_ID_PREFIX + "11" + SERVER_INSTANCE_POOL_PREFIX + 1,
            SERVER_INSTANCE_ID_PREFIX + "17" + SERVER_INSTANCE_POOL_PREFIX + 2));
  }
}

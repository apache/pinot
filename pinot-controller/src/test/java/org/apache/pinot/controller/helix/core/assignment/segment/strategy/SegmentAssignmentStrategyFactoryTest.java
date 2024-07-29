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
package org.apache.pinot.controller.helix.core.assignment.segment.strategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentTestUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests the {@link SegmentAssignmentStrategyFactory#getSegmentAssignmentStrategy} method
 */
public class SegmentAssignmentStrategyFactoryTest {

  private static final int NUM_REPLICAS = 3;
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String INSTANCE_PARTITIONS_NAME =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME);
  private static final String INSTANCE_NAME_PREFIX = "instance_";
  private static final int NUM_INSTANCES = 10;
  private static final List<String> INSTANCES =
      SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  private static final String RAW_TABLE_NAME_WITH_PARTITION = "testTableWithPartition";
  private static final String INSTANCE_PARTITIONS_NAME_WITH_PARTITION =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME_WITH_PARTITION);
  private static final int NUM_PARTITIONS = 3;
  private static final String PARTITION_COLUMN = "partitionColumn";

  private SegmentAssignmentStrategyFactoryTest() {
  }

  @Test
  public void testSegmentAssignmentStrategyFromTableConfig() {
    // Set segment assignment config map in table config for balanced num segment assignment strategy
    Map<String, SegmentAssignmentConfig> segmentAssignmentConfigMap = new HashMap<>();
    segmentAssignmentConfigMap.put(InstancePartitionsType.OFFLINE.toString(), new SegmentAssignmentConfig("Balanced"));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setSegmentAssignmentConfigMap(segmentAssignmentConfigMap).build();

    InstancePartitions instancePartitions = new InstancePartitions(INSTANCE_PARTITIONS_NAME);
    instancePartitions.setInstances(0, 0, INSTANCES);

    SegmentAssignmentStrategy segmentAssignmentStrategy = SegmentAssignmentStrategyFactory
        .getSegmentAssignmentStrategy(null, tableConfig, InstancePartitionsType.OFFLINE.toString(), instancePartitions);
    Assert.assertNotNull(segmentAssignmentStrategy);
    Assert.assertTrue(segmentAssignmentStrategy instanceof BalancedNumSegmentAssignmentStrategy);
  }

  @Test
  public void testSegmentAssignmentStrategyForDimTable() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setIsDimTable(true).build();
    SegmentAssignmentStrategy segmentAssignmentStrategy = SegmentAssignmentStrategyFactory
        .getSegmentAssignmentStrategy(null, tableConfig, InstancePartitionsType.OFFLINE.toString(), null);
    Assert.assertNotNull(segmentAssignmentStrategy);
    Assert.assertTrue(segmentAssignmentStrategy instanceof AllServersSegmentAssignmentStrategy);
  }

  @Test
  public void testBalancedNumSegmentAssignmentStrategyforOfflineTables() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

    InstancePartitions instancePartitions = new InstancePartitions(INSTANCE_PARTITIONS_NAME);
    instancePartitions.setInstances(0, 0, INSTANCES);

    SegmentAssignmentStrategy segmentAssignmentStrategy = SegmentAssignmentStrategyFactory
        .getSegmentAssignmentStrategy(null, tableConfig, InstancePartitionsType.OFFLINE.toString(), instancePartitions);
    Assert.assertNotNull(segmentAssignmentStrategy);
    Assert.assertTrue(segmentAssignmentStrategy instanceof BalancedNumSegmentAssignmentStrategy);
  }

  @Test
  public void testBalancedNumSegmentAssignmentStrategyForRealtimeTables() {
    Map<String, String> streamConfigs = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setStreamConfigs(streamConfigs).build();
    InstancePartitions instancePartitions = new InstancePartitions(INSTANCE_PARTITIONS_NAME);
    instancePartitions.setInstances(0, 0, INSTANCES);

    SegmentAssignmentStrategy segmentAssignmentStrategy =
        SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(null, tableConfig,
            InstancePartitionsType.COMPLETED.toString(), instancePartitions);
    Assert.assertNotNull(segmentAssignmentStrategy);

    Assert.assertTrue(segmentAssignmentStrategy instanceof BalancedNumSegmentAssignmentStrategy);
  }

  @Test
  public void testReplicaGroupSegmentAssignmentStrategyForBackwardCompatibility() {
    int numInstancesPerReplicaGroup = NUM_INSTANCES / NUM_REPLICAS;
    int numInstancesPerPartition = numInstancesPerReplicaGroup / NUM_REPLICAS;
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        new ReplicaGroupStrategyConfig(PARTITION_COLUMN, numInstancesPerPartition);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME_WITH_PARTITION)
        .setNumReplicas(NUM_REPLICAS).setSegmentAssignmentStrategy("ReplicaGroup")
        .setReplicaGroupStrategyConfig(replicaGroupStrategyConfig).build();

    // {
    //   0_0=[instance_0, instance_1], 1_0=[instance_2, instance_3], 2_0=[instance_4, instance_5],
    //   0_1=[instance_6, instance_7], 1_1=[instance_8, instance_9], 2_1=[instance_10, instance_11],
    //   0_2=[instance_12, instance_13], 1_2=[instance_14, instance_15], 2_2=[instance_16, instance_17]
    // }
    InstancePartitions instancePartitions = new InstancePartitions(INSTANCE_PARTITIONS_NAME_WITH_PARTITION);

    int instanceIdToAdd = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      for (int partitionId = 0; partitionId < NUM_PARTITIONS; partitionId++) {
        List<String> instancesForPartition = new ArrayList<>(numInstancesPerPartition);
        for (int i = 0; i < numInstancesPerPartition; i++) {
          instancesForPartition.add(INSTANCES.get(instanceIdToAdd++));
        }
        instancePartitions.setInstances(partitionId, replicaGroupId, instancesForPartition);
      }
    }

    SegmentAssignmentStrategy segmentAssignmentStrategy = SegmentAssignmentStrategyFactory
        .getSegmentAssignmentStrategy(null, tableConfig, InstancePartitionsType.OFFLINE.toString(), instancePartitions);
    Assert.assertNotNull(segmentAssignmentStrategy);
    Assert.assertTrue(segmentAssignmentStrategy instanceof ReplicaGroupSegmentAssignmentStrategy);
  }
}

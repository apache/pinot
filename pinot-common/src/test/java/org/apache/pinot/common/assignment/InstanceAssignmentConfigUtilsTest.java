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
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceConstraintConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InstanceAssignmentConfigUtilsTest {

  @Test
  public void testShouldRelocateCompletedSegmentsWhenInstancePartitionIsCompleted() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put(InstancePartitionsType.COMPLETED.toString(),
        getInstanceAssignmentConfig(InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap).build();
    Assert.assertTrue(InstanceAssignmentConfigUtils.shouldRelocateCompletedSegments(tableConfig));
  }

  @Test
  public void testShouldRelocateCompletedSegmentsWhenInstancePartitionIsConsuming() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put(InstancePartitionsType.CONSUMING.name(),
        getInstanceAssignmentConfig(InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR));
    TagOverrideConfig tagOverrideConfig = new TagOverrideConfig("broker", "Server");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap).setTagOverrideConfig(tagOverrideConfig).build();
    Assert.assertTrue(InstanceAssignmentConfigUtils.shouldRelocateCompletedSegments(tableConfig));
  }

  @Test
  public void testAllowInstanceAssignmentWithPreConfiguredInstancePartitions() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put(InstancePartitionsType.CONSUMING.name(),
        getInstanceAssignmentConfig(InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR));
    Map<InstancePartitionsType, String> instancePartitionsTypeStringMap = new HashMap<>();
    instancePartitionsTypeStringMap.put(InstancePartitionsType.CONSUMING, "testTable");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .setInstancePartitionsMap(instancePartitionsTypeStringMap).build();
    Assert.assertTrue(InstanceAssignmentConfigUtils
        .allowInstanceAssignment(tableConfig, InstancePartitionsType.CONSUMING));
  }


  //When instancePartitionsType is OFFLINE
  @Test
  public void testAllowInstanceAssignmentWithoutPreConfiguredInstancePartitionsOffline() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put(InstancePartitionsType.OFFLINE.name(),
        getInstanceAssignmentConfig(InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR));
    Map<InstancePartitionsType, String> instancePartitionsTypeStringMap = new HashMap<>();
    instancePartitionsTypeStringMap.put(InstancePartitionsType.CONSUMING, "testTable");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .setInstancePartitionsMap(instancePartitionsTypeStringMap).build();
    Assert.assertTrue(InstanceAssignmentConfigUtils
        .allowInstanceAssignment(tableConfig, InstancePartitionsType.OFFLINE));
  }

  //When instancePartitionsType is CONSUMING
  @Test
  public void testAllowInstanceAssignmentWithoutPreConfiguredInstancePartitionsCompleted() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put(InstancePartitionsType.COMPLETED.name(),
        getInstanceAssignmentConfig(InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR));
    Map<InstancePartitionsType, String> instancePartitionsTypeStringMap = new HashMap<>();
    instancePartitionsTypeStringMap.put(InstancePartitionsType.CONSUMING, "testTable");
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .setInstancePartitionsMap(instancePartitionsTypeStringMap).build();
    Assert.assertTrue(InstanceAssignmentConfigUtils
        .allowInstanceAssignment(tableConfig, InstancePartitionsType.COMPLETED));
  }

  //When instancePartitionsType is COMPLETED
  @Test
  public void testAllowInstanceAssignmentWithoutPreConfiguredInstancePartitionsConsuming() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put(InstancePartitionsType.CONSUMING.name(),
        getInstanceAssignmentConfig(InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR));
    Map<InstancePartitionsType, String> instancePartitionsTypeStringMap = new HashMap<>();
    instancePartitionsTypeStringMap.put(InstancePartitionsType.COMPLETED, "testTable");
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .setInstancePartitionsMap(instancePartitionsTypeStringMap).build();
    Assert.assertTrue(InstanceAssignmentConfigUtils
        .allowInstanceAssignment(tableConfig, InstancePartitionsType.CONSUMING));
  }

  @Test
  public void testGetInstanceAssignmentConfigWhenInstanceAssignmentConfig() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put(InstancePartitionsType.COMPLETED.name(),
        getInstanceAssignmentConfig(InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR));
    Map<InstancePartitionsType, String> instancePartitionsTypeStringMap = new HashMap<>();
    instancePartitionsTypeStringMap.put(InstancePartitionsType.COMPLETED, "testTable");
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .setInstancePartitionsMap(instancePartitionsTypeStringMap).build();
    Assert.assertEquals(InstanceAssignmentConfigUtils
            .getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.COMPLETED).getConstraintConfig()
            .getConstraints().get(0),
        "constraints1");
    Assert.assertEquals(InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig,
            InstancePartitionsType.COMPLETED).getReplicaGroupPartitionConfig().getNumInstancesPerPartition(),
        1);
  }

  @Test
  public void testGetInstanceAssignmentConfigWhenInstanceAssignmentConfigIsNotPresentAndPartitionColumnPresent() {

    TagOverrideConfig tagOverrideConfig = new TagOverrideConfig("broker", "Server");

    Map<InstancePartitionsType, String> instancePartitionsTypeStringMap = new HashMap<>();
    instancePartitionsTypeStringMap.put(InstancePartitionsType.OFFLINE, "offlineString");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setTagOverrideConfig(tagOverrideConfig).setInstancePartitionsMap(instancePartitionsTypeStringMap)
        .build();
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        new ReplicaGroupStrategyConfig("column1", 1);
    segmentsValidationAndRetentionConfig.setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);
    segmentsValidationAndRetentionConfig.setReplication("1");
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    IndexingConfig indexingConfig = new IndexingConfig();
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    ColumnPartitionConfig columnPartitionConfig = new ColumnPartitionConfig("column1", 1);
    columnPartitionConfigMap.put("column1", columnPartitionConfig);
    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(columnPartitionConfigMap);
    indexingConfig.setSegmentPartitionConfig(segmentPartitionConfig);
    tableConfig.setIndexingConfig(indexingConfig);
    Assert.assertEquals(InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig,
            InstancePartitionsType.OFFLINE).getReplicaGroupPartitionConfig().isReplicaGroupBased(), Boolean.TRUE);
    Assert.assertEquals(InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig,
            InstancePartitionsType.OFFLINE).getReplicaGroupPartitionConfig().getPartitionColumn(), "column1");
    Assert.assertEquals(InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig,
            InstancePartitionsType.OFFLINE).getReplicaGroupPartitionConfig().getNumInstancesPerPartition(), 1);
  }

  @Test
  public void testGetInstanceAssignmentConfigWhenInstanceAssignmentConfigIsNotPresentAndPartitionColumnNotPresent() {

    TagOverrideConfig tagOverrideConfig = new TagOverrideConfig("broker", "Server");

    Map<InstancePartitionsType, String> instancePartitionsTypeStringMap = new HashMap<>();
    instancePartitionsTypeStringMap.put(InstancePartitionsType.OFFLINE, "offlineString");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setTagOverrideConfig(tagOverrideConfig).setInstancePartitionsMap(instancePartitionsTypeStringMap)
        .build();
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        new ReplicaGroupStrategyConfig(null, 2);
    segmentsValidationAndRetentionConfig.setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);
    segmentsValidationAndRetentionConfig.setReplication("1");
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    IndexingConfig indexingConfig = new IndexingConfig();
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    ColumnPartitionConfig columnPartitionConfig = new ColumnPartitionConfig("column1", 1);
    columnPartitionConfigMap.put("column1", columnPartitionConfig);
    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(columnPartitionConfigMap);
    indexingConfig.setSegmentPartitionConfig(segmentPartitionConfig);
    tableConfig.setIndexingConfig(indexingConfig);
    Assert.assertEquals(InstanceAssignmentConfigUtils
        .getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE)
        .getReplicaGroupPartitionConfig().isReplicaGroupBased(), Boolean.TRUE);
    Assert.assertEquals(InstanceAssignmentConfigUtils
        .getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE)
        .getReplicaGroupPartitionConfig().getPartitionColumn(), null);
    Assert.assertEquals(InstanceAssignmentConfigUtils
        .getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE)
        .getReplicaGroupPartitionConfig().getNumInstancesPerReplicaGroup(), 2);
  }

  @Test
  public void testIsMirrorServerSetAssignment() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put(InstancePartitionsType.OFFLINE.name(),
        getInstanceAssignmentConfig(InstanceAssignmentConfig.PartitionSelector.MIRROR_SERVER_SET_PARTITION_SELECTOR));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .build();
    Assert.assertTrue(InstanceAssignmentConfigUtils.isMirrorServerSetAssignment(tableConfig,
        InstancePartitionsType.OFFLINE));
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

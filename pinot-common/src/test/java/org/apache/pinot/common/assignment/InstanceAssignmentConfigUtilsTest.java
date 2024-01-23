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
  public void testShouldRelocateCompletedSegmentsCompletedInstancePartition() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();

    InstanceTagPoolConfig instanceTagPoolConfig = new InstanceTagPoolConfig("tag",true,1, null);
    List<String> constraints = new ArrayList<>();
    constraints.add("constraints1");
    InstanceConstraintConfig instanceConstraintConfig = new InstanceConstraintConfig(constraints);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 1,1,1,1,1,true,null);
    InstanceAssignmentConfig instanceAssignmentConfig = new InstanceAssignmentConfig(instanceTagPoolConfig,instanceConstraintConfig,
        instanceReplicaGroupPartitionConfig, InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.name());

    //check map key
    instanceAssignmentConfigMap.put(InstancePartitionsType.COMPLETED.toString(), instanceAssignmentConfig);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap).build();
    Assert.assertTrue(InstanceAssignmentConfigUtils.shouldRelocateCompletedSegments(tableConfig));
  }

  @Test
  public void testShouldRelocateCompletedSegmentsConsuming() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();

    InstanceTagPoolConfig instanceTagPoolConfig = new InstanceTagPoolConfig("tag",true,1, null);
    List<String> constraints = new ArrayList<>();
    constraints.add("constraints1");
    InstanceConstraintConfig instanceConstraintConfig = new InstanceConstraintConfig(constraints);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 1,1,1,1,1,true,null);
    InstanceAssignmentConfig instanceAssignmentConfig = new InstanceAssignmentConfig(instanceTagPoolConfig,instanceConstraintConfig,
        instanceReplicaGroupPartitionConfig, InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.name());

    //check map key
    instanceAssignmentConfigMap.put(InstancePartitionsType.CONSUMING.name(), instanceAssignmentConfig);
    TagOverrideConfig tagOverrideConfig = new TagOverrideConfig("broker", "Server");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap).setTagOverrideConfig(tagOverrideConfig).build();
    Assert.assertTrue(InstanceAssignmentConfigUtils.shouldRelocateCompletedSegments(tableConfig));
  }

  //try false for above, and null as well.

  @Test
  public void testAllowInstanceAssignmentWithPreConfiguredInstancePartitions() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();

    InstanceTagPoolConfig instanceTagPoolConfig = new InstanceTagPoolConfig("tag",true,1, null);
    List<String> constraints = new ArrayList<>();
    constraints.add("constraints1");
    InstanceConstraintConfig instanceConstraintConfig = new InstanceConstraintConfig(constraints);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 1,1,1,1,1,true,null);
    InstanceAssignmentConfig instanceAssignmentConfig = new InstanceAssignmentConfig(instanceTagPoolConfig,instanceConstraintConfig,
        instanceReplicaGroupPartitionConfig, InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.name());

    //check map key
    instanceAssignmentConfigMap.put(InstancePartitionsType.CONSUMING.name(), instanceAssignmentConfig);

    Map<InstancePartitionsType, String> instancePartitionsTypeStringMap = new HashMap<>();
    instancePartitionsTypeStringMap.put(InstancePartitionsType.CONSUMING, "testTable");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .setInstancePartitionsMap(instancePartitionsTypeStringMap).build();

    Assert.assertTrue(InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig, InstancePartitionsType.CONSUMING));
  }

  @Test
  public void testAllowInstanceAssignmentWithoutPreConfiguredInstancePartitionsOFFLINE() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();

    InstanceTagPoolConfig instanceTagPoolConfig = new InstanceTagPoolConfig("tag",true,1, null);
    List<String> constraints = new ArrayList<>();
    constraints.add("constraints1");
    InstanceConstraintConfig instanceConstraintConfig = new InstanceConstraintConfig(constraints);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 1,1,1,1,1,true,null);
    InstanceAssignmentConfig instanceAssignmentConfig = new InstanceAssignmentConfig(instanceTagPoolConfig,instanceConstraintConfig,
        instanceReplicaGroupPartitionConfig, InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.name());

    //check map key
    instanceAssignmentConfigMap.put(InstancePartitionsType.OFFLINE.name(), instanceAssignmentConfig);

    Map<InstancePartitionsType, String> instancePartitionsTypeStringMap = new HashMap<>();
    instancePartitionsTypeStringMap.put(InstancePartitionsType.CONSUMING, "testTable");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .setInstancePartitionsMap(instancePartitionsTypeStringMap).build();
    Assert.assertTrue(InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig, InstancePartitionsType.CONSUMING));
  }

  @Test
  public void testAllowInstanceAssignmentWithoutPreConfiguredInstancePartitionsCONSUMING() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();

    InstanceTagPoolConfig instanceTagPoolConfig = new InstanceTagPoolConfig("tag",true,1, null);
    List<String> constraints = new ArrayList<>();
    constraints.add("constraints1");
    InstanceConstraintConfig instanceConstraintConfig = new InstanceConstraintConfig(constraints);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 1,1,1,1,1,true,null);
    InstanceAssignmentConfig instanceAssignmentConfig = new InstanceAssignmentConfig(instanceTagPoolConfig,instanceConstraintConfig,
        instanceReplicaGroupPartitionConfig, InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.name());

    //check map key
    instanceAssignmentConfigMap.put(InstancePartitionsType.CONSUMING.name(), instanceAssignmentConfig);

    Map<InstancePartitionsType, String> instancePartitionsTypeStringMap = new HashMap<>();
    instancePartitionsTypeStringMap.put(InstancePartitionsType.CONSUMING, "testTable");
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .setInstancePartitionsMap(instancePartitionsTypeStringMap).build();
    Assert.assertTrue(InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig, InstancePartitionsType.CONSUMING));
  }

  @Test
  public void testAllowInstanceAssignmentWithoutPreConfiguredInstancePartitionsCOMPLETED() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();

    InstanceTagPoolConfig instanceTagPoolConfig = new InstanceTagPoolConfig("tag",true,1, null);
    List<String> constraints = new ArrayList<>();
    constraints.add("constraints1");
    InstanceConstraintConfig instanceConstraintConfig = new InstanceConstraintConfig(constraints);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 1,1,1,1,1,true,null);
    InstanceAssignmentConfig instanceAssignmentConfig = new InstanceAssignmentConfig(instanceTagPoolConfig,instanceConstraintConfig,
        instanceReplicaGroupPartitionConfig, InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.name());

    //check map key
    instanceAssignmentConfigMap.put(InstancePartitionsType.COMPLETED.name(), instanceAssignmentConfig);

    Map<InstancePartitionsType, String> instancePartitionsTypeStringMap = new HashMap<>();
    instancePartitionsTypeStringMap.put(InstancePartitionsType.COMPLETED, "testTable");
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .setInstancePartitionsMap(instancePartitionsTypeStringMap).build();
    Assert.assertTrue(InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig, InstancePartitionsType.COMPLETED));
  }

  //cant set up exception data/ find a scenario where line 77 will be reached
//  @Test(expectedExceptions = IllegalStateException.class)
//  public void testAllowInstanceAssignmentWithoutPreConfiguredInstancePartitionsNullInstancePartitionsType() {
//    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
//
//    InstanceTagPoolConfig instanceTagPoolConfig = new InstanceTagPoolConfig("tag",true,1, null);
//    List<String> constraints = new ArrayList<>();
//    constraints.add("constraints1");
//    InstanceConstraintConfig instanceConstraintConfig = new InstanceConstraintConfig(constraints);
//    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
//        new InstanceReplicaGroupPartitionConfig(true, 1,1,1,1,1,true,null);
//    InstanceAssignmentConfig instanceAssignmentConfig = new InstanceAssignmentConfig(instanceTagPoolConfig,instanceConstraintConfig,
//        instanceReplicaGroupPartitionConfig, InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.name());
//
//    //check map key
//    instanceAssignmentConfigMap.put(InstancePartitionsType.COMPLETED.name(), instanceAssignmentConfig);
//
//    Map<InstancePartitionsType, String> instancePartitionsTypeStringMap = new HashMap<>();
//    instancePartitionsTypeStringMap.put(InstancePartitionsType.CONSUMING, "testTable");
//    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
//        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
//        .setInstancePartitionsMap(instancePartitionsTypeStringMap).build();
//    Assert.assertTrue(InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig,
//        InstancePartitionsType.valueOf("null")));
//  }

  @Test
  public void testGetInstanceAssignmentConfigWhenInstanceAssignmentConfig() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();

    InstanceTagPoolConfig instanceTagPoolConfig = new InstanceTagPoolConfig("tag",true,1, null);
    List<String> constraints = new ArrayList<>();
    constraints.add("constraints1");
    InstanceConstraintConfig instanceConstraintConfig = new InstanceConstraintConfig(constraints);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 1,1,1,1,10,true,null);
    InstanceAssignmentConfig instanceAssignmentConfig = new InstanceAssignmentConfig(instanceTagPoolConfig,instanceConstraintConfig,
        instanceReplicaGroupPartitionConfig, InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.name());

    //check map key
    instanceAssignmentConfigMap.put(InstancePartitionsType.COMPLETED.name(), instanceAssignmentConfig);

    Map<InstancePartitionsType, String> instancePartitionsTypeStringMap = new HashMap<>();
    instancePartitionsTypeStringMap.put(InstancePartitionsType.COMPLETED, "testTable");
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .setInstancePartitionsMap(instancePartitionsTypeStringMap).build();
    Assert.assertEquals(InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.COMPLETED)
            .getConstraintConfig().getConstraints().get(0),
        "constraints1");
    //Check if you can assert entire objects with values in line 214
    Assert.assertEquals(InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.COMPLETED)
            .getReplicaGroupPartitionConfig().getNumInstancesPerPartition(), 10);

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
    Assert.assertEquals(InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE)
            .getReplicaGroupPartitionConfig().isReplicaGroupBased(), Boolean.TRUE);
    Assert.assertEquals(InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE)
        .getReplicaGroupPartitionConfig().getPartitionColumn(), "column1");
    Assert.assertEquals(InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE)
        .getReplicaGroupPartitionConfig().getNumInstancesPerPartition(), 1);
    //Check if we want to asset all the objects for getReplicaGroupPartitionConfig()
  }

  //Do we need null check in L113 in utils?
  //Below test same, only 253, 267,269 changed

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
    Assert.assertEquals(InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE)
        .getReplicaGroupPartitionConfig().isReplicaGroupBased(), Boolean.TRUE);
    Assert.assertEquals(InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE)
        .getReplicaGroupPartitionConfig().getPartitionColumn(), null);
    Assert.assertEquals(InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE)
        .getReplicaGroupPartitionConfig().getNumInstancesPerReplicaGroup(), 2);
    //Check if we want to asset all the objects for getReplicaGroupPartitionConfig()
  }

  @Test
  public void testIsMirrorServerSetAssignment() {

    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    InstanceTagPoolConfig instanceTagPoolConfig = new InstanceTagPoolConfig("tag",true,1, null);
    List<String> constraints = new ArrayList<>();
    constraints.add("constraints1");
    InstanceConstraintConfig instanceConstraintConfig = new InstanceConstraintConfig(constraints);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 1,1,1,1,1,true,null);

    InstanceAssignmentConfig instanceAssignmentConfig = new InstanceAssignmentConfig(instanceTagPoolConfig, instanceConstraintConfig, instanceReplicaGroupPartitionConfig, InstanceAssignmentConfig.PartitionSelector.MIRROR_SERVER_SET_PARTITION_SELECTOR.name());
    instanceAssignmentConfigMap.put(InstancePartitionsType.OFFLINE.name(), instanceAssignmentConfig);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .build();

    Assert.assertTrue(InstanceAssignmentConfigUtils.isMirrorServerSetAssignment(tableConfig, InstancePartitionsType.OFFLINE));

  }





}

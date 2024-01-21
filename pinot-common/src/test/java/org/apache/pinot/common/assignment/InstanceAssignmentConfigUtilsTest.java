package org.apache.pinot.common.assignment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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


}

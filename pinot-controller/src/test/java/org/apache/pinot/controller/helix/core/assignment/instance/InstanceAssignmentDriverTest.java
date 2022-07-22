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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceConstraintConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class InstanceAssignmentDriverTest {
  private static final String TEST_SERVER_TAG = "ServerTag";
  private static final String TABLE_NAME = "test_table";
  private static final String GROUP_NAME = "group1";
  private static final InstanceAssignmentConfig ASSIGNMENT_CONFIG = new InstanceAssignmentConfig(
      new InstanceTagPoolConfig(TEST_SERVER_TAG, false, 1, Collections.singletonList(1)),
      new InstanceConstraintConfig(new ArrayList<>()), new InstanceReplicaGroupPartitionConfig(true,
      4,
      2,
      2,
      1,
      2,
      false));
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(TABLE_NAME)
      .setInstanceAssignmentConfigMap(ImmutableMap.of(InstancePartitionsType.OFFLINE, ASSIGNMENT_CONFIG))
      .build();
  private static final List<InstanceConfig> INSTANCE_CONFIGS = new ArrayList<>();

  @BeforeClass
  public void setUp() {
    INSTANCE_CONFIGS.add(new InstanceConfig("instance-1"));
    INSTANCE_CONFIGS.add(new InstanceConfig("instance-2"));
    INSTANCE_CONFIGS.add(new InstanceConfig("instance-3"));
    INSTANCE_CONFIGS.add(new InstanceConfig("instance-4"));
    INSTANCE_CONFIGS.forEach(x -> {
      x.addTag(TEST_SERVER_TAG);
    });
  }

  // Test for InstanceAssignmentDriver::assignInstances, which is used for assigning instances to tables.
  @Test
  public void testAssignInstances() {
    // Test assignment and ensure InstancePartitions name is correct, and the number of replica-groups and partitions
    // in the result are consistent with InstanceAssignmentConfig.
    InstanceAssignmentDriver instanceAssignmentDriver = new InstanceAssignmentDriver(TABLE_CONFIG);
    InstancePartitions tableInstancePartitions =
        instanceAssignmentDriver.assignInstances(InstancePartitionsType.OFFLINE, INSTANCE_CONFIGS, null);
    Assert.assertEquals(InstancePartitionsType.OFFLINE.getInstancePartitionsName(TABLE_NAME),
        tableInstancePartitions.getInstancePartitionsName());
    Assert.assertEquals(1, tableInstancePartitions.getNumPartitions());
    Assert.assertEquals(2, tableInstancePartitions.getNumReplicaGroups());
    Assert.assertEquals(2, tableInstancePartitions.getInstances(0, 0).size());

    // If a table is part of a table-group, we shouldn't allow calls to go through. Caller should instead use
    // assignInstances.
    TableConfig tableConfigForTableInGroup = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .build();
    instanceAssignmentDriver = new InstanceAssignmentDriver(tableConfigForTableInGroup);
    try {
      instanceAssignmentDriver.assignInstances(InstancePartitionsType.OFFLINE, INSTANCE_CONFIGS, null);
      Assert.fail("assignInstances should fail if table is in group");
    } catch (IllegalStateException ignored) {
    }
  }

  // Test for InstanceAssignmentDriver::assignInstancesToGroup, which is used for assigning instances to a table-group.
  @Test
  public void testAssignInstancesToGroup() {
    // Ensure InstancePartitions name is correct, and the returned instance partitions have replica-groups/partitions
    // consistent with InstanceAssignmentConfig.
    InstancePartitions groupInstancePartitions = InstanceAssignmentDriver.assignInstancesToGroup(GROUP_NAME,
        INSTANCE_CONFIGS, ASSIGNMENT_CONFIG);
    Assert.assertEquals(InstancePartitionsUtils.getGroupInstancePartitionsName(GROUP_NAME),
        groupInstancePartitions.getInstancePartitionsName());
    Assert.assertEquals(1, groupInstancePartitions.getNumPartitions());
    Assert.assertEquals(2, groupInstancePartitions.getNumReplicaGroups());
    Assert.assertEquals(2, groupInstancePartitions.getInstances(0, 0).size());
  }
}

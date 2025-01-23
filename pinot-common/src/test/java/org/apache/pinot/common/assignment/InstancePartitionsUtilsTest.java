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
  public void testShouldFetchPreConfiguredTenantInstancePartitions() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put(InstancePartitionsType.OFFLINE.name(),
        getInstanceAssignmentConfig(InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap).build();

    Assert.assertTrue(InstancePartitionsUtils.shouldFetchPreConfiguredTenantInstancePartitions(tableConfig, InstancePartitionsType.OFFLINE));
  }

  @Test
  public void testPreConfiguredTenantInstancePartitionsMirrorServerSet() {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put(InstancePartitionsType.OFFLINE.name(),
        getInstanceAssignmentConfig(InstanceAssignmentConfig.PartitionSelector.MIRROR_SERVER_SET_PARTITION_SELECTOR));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap).build();

    Assert.assertFalse(InstancePartitionsUtils.shouldFetchPreConfiguredTenantInstancePartitions(tableConfig, InstancePartitionsType.OFFLINE));
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

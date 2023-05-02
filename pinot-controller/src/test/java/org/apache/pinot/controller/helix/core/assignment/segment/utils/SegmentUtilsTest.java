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
package org.apache.pinot.controller.helix.core.assignment.segment.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.controller.helix.core.assignment.utils.SegmentUtils;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SegmentUtilsTest {
  private static final String TABLE_NAME = "TestTable";
  private static final String PARTITION_COLUMN = "partitionColumn";
  InstanceAssignmentConfig _instanceAssignmentConfig;
  ReplicaGroupStrategyConfig _replicaGroupStrategyConfig;

  @BeforeClass
  public void setUp() {
    InstanceReplicaGroupPartitionConfig replicaPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0,
            1, 1, 1,
            0, false, PARTITION_COLUMN);

    _instanceAssignmentConfig = new InstanceAssignmentConfig(
        new InstanceTagPoolConfig("test_tag", false, 1, null),
        null, replicaPartitionConfig);

    _replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig(PARTITION_COLUMN, 1);
  }
 //@Test
  public void testGetPartitionColumnFromInstanceAssignmentConfig() {
    Map<String, InstanceAssignmentConfig> configMap = new HashMap<>();
    configMap.put(TableType.REALTIME.toString(), _instanceAssignmentConfig);

    TableConfig tableConfig =
       new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
           .setInstanceAssignmentConfigMap(configMap).build();
    Assert.assertEquals(PARTITION_COLUMN, SegmentUtils.getPartitionColumn(tableConfig));

   TableConfig tableConfig1 =
       new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
           .setInstanceAssignmentConfigMap(configMap).build();
   Assert.assertNull(SegmentUtils.getPartitionColumn(tableConfig1)); //since no key with 'OFFLINE'
  }

  @Test
  public void testGetPartitionColumnWithoutAnyConfig() {
    // without instanceAssignmentConfigMap
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).build();
    Assert.assertNull(SegmentUtils.getPartitionColumn(tableConfig));
  }

  @Test
  public void testGetPartitionColumnWithReplicaGroupConfig() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).build();

    // setting up ReplicaGroupStrategyConfig for backward compatibility test.
    SegmentsValidationAndRetentionConfig validationConfig = new SegmentsValidationAndRetentionConfig();
    validationConfig.setReplicaGroupStrategyConfig(_replicaGroupStrategyConfig);
    tableConfig.setValidationConfig(validationConfig);

    Assert.assertEquals(PARTITION_COLUMN, SegmentUtils.getPartitionColumn(tableConfig));
  }
}

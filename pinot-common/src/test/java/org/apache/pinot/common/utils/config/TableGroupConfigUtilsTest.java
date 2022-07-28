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
package org.apache.pinot.common.utils.config;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.pinot.spi.config.table.TableGroupConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceConstraintConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TableGroupConfigUtilsTest {
  private static final String TEST_SERVER_TAG = "ServerTag";
  private static final InstanceAssignmentConfig ASSIGNMENT_CONFIG = new InstanceAssignmentConfig(
      new InstanceTagPoolConfig(TEST_SERVER_TAG, false, 1, Collections.singletonList(1)),
      new InstanceConstraintConfig(new ArrayList<>()), new InstanceReplicaGroupPartitionConfig(true,
      4,
      2,
      2,
      1,
      2,
      false));

  @Test
  public void testConsistentZNRecordConversion()
      throws IOException {
    Map<InstancePartitionsType, InstanceAssignmentConfig> instanceAssignmentConfigMap = ImmutableMap.of(
        InstancePartitionsType.OFFLINE, ASSIGNMENT_CONFIG,
        InstancePartitionsType.CONSUMING, ASSIGNMENT_CONFIG);
    TableGroupConfig tableGroupConfig = new TableGroupConfig("test-group", true, instanceAssignmentConfigMap);
    ZNRecord znRecord = TableGroupConfigUtils.toZNRecord(tableGroupConfig);
    TableGroupConfig tableGroupConfigAfterConversion = TableGroupConfigUtils.fromZNRecord(znRecord);
    Assert.assertEquals(tableGroupConfigAfterConversion, tableGroupConfig);
  }
}

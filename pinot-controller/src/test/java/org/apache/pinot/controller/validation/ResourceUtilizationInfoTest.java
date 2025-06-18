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
package org.apache.pinot.controller.validation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.restlet.resources.DiskUsageInfo;
import org.apache.pinot.common.restlet.resources.PrimaryKeyCountInfo;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ResourceUtilizationInfoTest {

  private static final String INSTANCE_ID_1 = "server-1";
  private static final String INSTANCE_ID_2 = "server-2";

  private DiskUsageInfo _diskUsageInfo1;
  private DiskUsageInfo _diskUsageInfo2;

  private PrimaryKeyCountInfo _primaryKeyCountInfo1;
  private PrimaryKeyCountInfo _primaryKeyCountInfo2;

  @BeforeMethod
  public void setUp() {
    // Create sample DiskUsageInfo objects
    _diskUsageInfo1 = new DiskUsageInfo(INSTANCE_ID_1, "/path/to/disk1", 1000L, 500L, System.currentTimeMillis());
    _diskUsageInfo2 = new DiskUsageInfo(INSTANCE_ID_2, "/path/to/disk2", 2000L, 1500L, System.currentTimeMillis());

    // Create sample PrimaryKeyCountInfo objects
    _primaryKeyCountInfo1 = new PrimaryKeyCountInfo(INSTANCE_ID_1, 42L, Set.of("table_REALTIME"),
        System.currentTimeMillis());
    _primaryKeyCountInfo2 = new PrimaryKeyCountInfo(INSTANCE_ID_2, 4242L, Set.of("table_REALTIME", "table2_REALTIME"),
        System.currentTimeMillis());
  }

  @Test
  public void testDiskUsageInfo() {
    // Set disk usage info for multiple instances
    Map<String, DiskUsageInfo> diskUsageInfoMap = new HashMap<>();
    diskUsageInfoMap.put(INSTANCE_ID_1, _diskUsageInfo1);
    diskUsageInfoMap.put(INSTANCE_ID_2, _diskUsageInfo2);
    ResourceUtilizationInfo.setDiskUsageInfo(diskUsageInfoMap);

    // Validate instance 1
    DiskUsageInfo diskUsageInfoInstance1 = ResourceUtilizationInfo.getDiskUsageInfo(INSTANCE_ID_1);
    Assert.assertNotNull(diskUsageInfoInstance1);
    Assert.assertEquals(diskUsageInfoInstance1.getTotalSpaceBytes(), 1000L);
    Assert.assertEquals(diskUsageInfoInstance1.getUsedSpaceBytes(), 500L);

    // Validate instance 2
    DiskUsageInfo diskUsageInfoInstance2 = ResourceUtilizationInfo.getDiskUsageInfo(INSTANCE_ID_2);
    Assert.assertNotNull(diskUsageInfoInstance2);
    Assert.assertEquals(diskUsageInfoInstance2.getTotalSpaceBytes(), 2000L);
    Assert.assertEquals(diskUsageInfoInstance2.getUsedSpaceBytes(), 1500L);
  }

  @Test
  public void testPrimaryKeyCountInfo() {
    // Set disk usage info for multiple instances
    Map<String, PrimaryKeyCountInfo> primaryKeyCountInfoMap = new HashMap<>();
    primaryKeyCountInfoMap.put(INSTANCE_ID_1, _primaryKeyCountInfo1);
    primaryKeyCountInfoMap.put(INSTANCE_ID_2, _primaryKeyCountInfo2);
    ResourceUtilizationInfo.setPrimaryKeyCountInfo(primaryKeyCountInfoMap);

    // Validate instance 1
    PrimaryKeyCountInfo primaryKeyCountInfo1 = ResourceUtilizationInfo.getPrimaryKeyCountInfo(INSTANCE_ID_1);
    Assert.assertNotNull(primaryKeyCountInfo1);
    Assert.assertEquals(primaryKeyCountInfo1.getInstanceId(), INSTANCE_ID_1);
    Assert.assertEquals(primaryKeyCountInfo1.getNumPrimaryKeys(), 42L);
    Assert.assertNotNull(primaryKeyCountInfo1.getTablesWithPrimaryKeys());
    Assert.assertEquals(primaryKeyCountInfo1.getTablesWithPrimaryKeys().size(), 1);
    Assert.assertTrue(primaryKeyCountInfo1.getTablesWithPrimaryKeys().contains("table_REALTIME"));

    // Validate instance 2
    PrimaryKeyCountInfo primaryKeyCountInfo2 = ResourceUtilizationInfo.getPrimaryKeyCountInfo(INSTANCE_ID_2);
    Assert.assertNotNull(primaryKeyCountInfo2);
    Assert.assertEquals(primaryKeyCountInfo2.getInstanceId(), INSTANCE_ID_2);
    Assert.assertEquals(primaryKeyCountInfo2.getNumPrimaryKeys(), 4242L);
    Assert.assertNotNull(primaryKeyCountInfo2.getTablesWithPrimaryKeys());
    Assert.assertEquals(primaryKeyCountInfo2.getTablesWithPrimaryKeys().size(), 2);
    Assert.assertTrue(primaryKeyCountInfo2.getTablesWithPrimaryKeys().contains("table_REALTIME"));
    Assert.assertTrue(primaryKeyCountInfo2.getTablesWithPrimaryKeys().contains("table2_REALTIME"));
  }
}

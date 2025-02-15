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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.restlet.resources.DiskUsageInfo;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class DiskUtilizationCheckerTest {

  private static final String DISK_UTILIZATION_PATH = "/disk/utilization/path";
  private PinotHelixResourceManager _helixResourceManager;
  private ControllerConf _controllerConf;
  private DiskUtilizationChecker _diskUtilizationChecker;

  @BeforeMethod
  public void setUp() {
    _helixResourceManager = mock(PinotHelixResourceManager.class);
    _controllerConf = mock(ControllerConf.class);

    when(_controllerConf.getDiskUtilizationPath()).thenReturn(DISK_UTILIZATION_PATH);
    when(_controllerConf.getDiskUtilizationThreshold()).thenReturn(0.8);
    when(_controllerConf.getDiskUtilizationCheckTimeoutMs()).thenReturn(5000);
    when(_controllerConf.getResourceUtilizationCheckerFrequency()).thenReturn(120L);

    _diskUtilizationChecker = new DiskUtilizationChecker(_helixResourceManager, _controllerConf);
  }

  @Test
  public void testIsDiskUtilizationWithinLimitsNullOrEmptyTableName() {
    Assert.assertThrows(IllegalArgumentException.class,
        () -> _diskUtilizationChecker.isDiskUtilizationWithinLimits(null));
    Assert.assertThrows(IllegalArgumentException.class,
        () -> _diskUtilizationChecker.isDiskUtilizationWithinLimits(""));
  }

  @Test
  public void testIsDiskUtilizationWithinLimitsNonExistentOfflineTable() {
    String tableName = "test_OFFLINE";
    when(_helixResourceManager.getOfflineTableConfig(tableName)).thenReturn(null);

    boolean result = _diskUtilizationChecker.isDiskUtilizationWithinLimits(tableName);
    Assert.assertTrue(result);
  }

  @Test
  public void testIsDiskUtilizationWithinLimitsNonExistentRealtimeTable() {
    String tableName = "test_REALTIME";
    when(_helixResourceManager.getOfflineTableConfig(tableName)).thenReturn(null);

    boolean result = _diskUtilizationChecker.isDiskUtilizationWithinLimits(tableName);
    Assert.assertTrue(result);
  }

  @Test
  public void testIsDiskUtilizationWithinLimitsValidOfflineTable() {
    String tableName = "test_OFFLINE";

    TableConfig mockTableConfig = mock(TableConfig.class);
    when(_helixResourceManager.getOfflineTableConfig(tableName)).thenReturn(mockTableConfig);

    List<String> mockInstances = Arrays.asList("server1", "server2");
    when(_helixResourceManager.getServerInstancesForTable(tableName, TableType.OFFLINE)).thenReturn(mockInstances);

    // Mock disk usage
    Map<String, DiskUsageInfo> diskUsageInfoMap = new HashMap<>();
    DiskUsageInfo diskUsageInfo1 =
        new DiskUsageInfo("server1", DISK_UTILIZATION_PATH, 1000L, 500L, System.currentTimeMillis());
    diskUsageInfoMap.put("server1", diskUsageInfo1);

    DiskUsageInfo diskUsageInfo2 =
        new DiskUsageInfo("server2", DISK_UTILIZATION_PATH, 2000L, 1000L, System.currentTimeMillis());
    diskUsageInfoMap.put("server2", diskUsageInfo2);
    ResourceUtilizationInfo.setDiskUsageInfo(diskUsageInfoMap);

    boolean result = _diskUtilizationChecker.isDiskUtilizationWithinLimits(tableName);
    Assert.assertTrue(result);
  }

  @Test
  public void testIsDiskUtilizationWithinLimitsAboveThreshold() {
    String tableName = "test_OFFLINE";

    TableConfig mockTableConfig = mock(TableConfig.class);
    when(_helixResourceManager.getTableConfig(tableName)).thenReturn(mockTableConfig);

    List<String> mockInstances = Arrays.asList("server1", "server2");
    when(_helixResourceManager.getServerInstancesForTable(tableName, TableType.OFFLINE)).thenReturn(mockInstances);

    // Mock disk usage with high utilization
    Map<String, DiskUsageInfo> diskUsageInfoMap = new HashMap<>();
    DiskUsageInfo diskUsageInfo1 = new DiskUsageInfo("server1", DISK_UTILIZATION_PATH, 1000L, 900L,
        System.currentTimeMillis()); // Above threshold (90%)
    diskUsageInfoMap.put("server1", diskUsageInfo1);

    DiskUsageInfo diskUsageInfo2 = new DiskUsageInfo("server2", DISK_UTILIZATION_PATH, 2000L, 1900L,
        System.currentTimeMillis()); // Below threshold (50%)
    diskUsageInfoMap.put("server2", diskUsageInfo2);
    ResourceUtilizationInfo.setDiskUsageInfo(diskUsageInfoMap);

    boolean result = _diskUtilizationChecker.isDiskUtilizationWithinLimits(tableName);
    Assert.assertFalse(result);
  }

  @Test
  public void testComputeDiskUtilizationValidInstances()
      throws InvalidConfigException {
    Set<String> instances = new HashSet<>(Arrays.asList("server1", "server2"));

    // Mock admin endpoints
    BiMap<String, String> instanceAdminEndpoints = HashBiMap.create();
    instanceAdminEndpoints.put("server1", "http://server1");
    instanceAdminEndpoints.put("server2", "http://server2");
    when(_helixResourceManager.getDataInstanceAdminEndpoints(instances)).thenReturn(instanceAdminEndpoints);

    // Mock responses
    Map<String, String> responseMap = new HashMap<>();
    responseMap.put("http://server1" + DiskUtilizationChecker.DISK_UTILIZATION_API_PATH,
        "{ \"instanceId\": \"server1\", \"totalSpaceBytes\": 1000, \"usedSpaceBytes\": 500 }");
    responseMap.put("http://server2" + DiskUtilizationChecker.DISK_UTILIZATION_API_PATH,
        "{ \"instanceId\": \"server2\", \"totalSpaceBytes\": 2000, \"usedSpaceBytes\": 1500 }");

    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        new CompletionServiceHelper.CompletionServiceResponse();
    serviceResponse._httpResponses = responseMap;

    CompletionServiceHelper completionServiceHelper = mock(CompletionServiceHelper.class);
    when(completionServiceHelper.doMultiGetRequest(anyList(), anyString(), anyBoolean(), anyMap(), anyInt(),
        anyString())).thenReturn(serviceResponse);

    _diskUtilizationChecker.computeDiskUtilization(instanceAdminEndpoints.inverse(), completionServiceHelper);

    DiskUsageInfo diskUsageInfo1 = ResourceUtilizationInfo.getDiskUsageInfo("server1");
    DiskUsageInfo diskUsageInfo2 = ResourceUtilizationInfo.getDiskUsageInfo("server2");

    Assert.assertNotNull(diskUsageInfo1);
    Assert.assertEquals(diskUsageInfo1.getTotalSpaceBytes(), 1000L);
    Assert.assertEquals(diskUsageInfo1.getUsedSpaceBytes(), 500L);

    Assert.assertNotNull(diskUsageInfo2);
    Assert.assertEquals(diskUsageInfo2.getTotalSpaceBytes(), 2000L);
    Assert.assertEquals(diskUsageInfo2.getUsedSpaceBytes(), 1500L);
  }
}

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
import org.apache.pinot.common.restlet.resources.PrimaryKeyCountInfo;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class PrimaryKeyCountCheckerTest {

  private PinotHelixResourceManager _helixResourceManager;
  private PrimaryKeyCountChecker _primaryKeyCountChecker;

  @BeforeMethod
  public void setUp() {
    _helixResourceManager = mock(PinotHelixResourceManager.class);
    ControllerConf controllerConf = mock(ControllerConf.class);

    when(controllerConf.getPrimaryKeyCountThreshold()).thenReturn(200L);
    when(controllerConf.getPrimaryKeyCountCheckTimeoutMs()).thenReturn(5000);
    when(controllerConf.getResourceUtilizationCheckerFrequency()).thenReturn(120L);

    _primaryKeyCountChecker = new PrimaryKeyCountChecker(_helixResourceManager, controllerConf);
  }

  @Test
  public void testNumberOfPrimaryKeysCheckerWithNullOrEmptyTableName() {
    Assert.assertThrows(IllegalArgumentException.class,
        () -> _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(null, true));
    Assert.assertThrows(IllegalArgumentException.class,
        () -> _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits("", true));

    // If skipRealtimeIngestion = false (i.e. code paths initiated by the Minion side) no exception should be thrown,
    // and true should always be returned
    boolean value = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(null, false);
    Assert.assertTrue(value);
    value = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits("", false);
    Assert.assertTrue(value);
  }


  @Test
  public void testNumberOfPrimaryKeysCheckerWithNonExistentOfflineTable() {
    String tableName = "test_OFFLINE";
    when(_helixResourceManager.getTableConfig(tableName)).thenReturn(null);

    boolean result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, true);
    Assert.assertTrue(result);

    result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, false);
    Assert.assertTrue(result);
  }

  @Test
  public void testNumberOfPrimaryKeysCheckerWithNonExistentRealtimeTable() {
    String tableName = "test_REALTIME";
    when(_helixResourceManager.getTableConfig(tableName)).thenReturn(null);

    boolean result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, true);
    Assert.assertTrue(result);

    result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, false);
    Assert.assertTrue(result);
  }

  @Test
  public void testNumberOfPrimaryKeysCheckerWithValidOfflineTable() {
    String tableName = "test_OFFLINE";

    TableConfig mockTableConfig = mock(TableConfig.class);
    when(_helixResourceManager.getTableConfig(tableName)).thenReturn(mockTableConfig);

    List<String> mockInstances = Arrays.asList("server1", "server2");
    when(_helixResourceManager.getServerInstancesForTable(tableName, TableType.OFFLINE)).thenReturn(mockInstances);

    // Mock primary key counts, set count to be higher threshold (to validate that we skip this check for OFFLINE
    // tables)
    Map<String, PrimaryKeyCountInfo> primaryKeyCountInfoMap = new HashMap<>();
    PrimaryKeyCountInfo primaryKeyCountInfo1 =
        new PrimaryKeyCountInfo("server1", 1000L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server1", primaryKeyCountInfo1);

    PrimaryKeyCountInfo primaryKeyCountInfo2 =
        new PrimaryKeyCountInfo("server2", 2000L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server2", primaryKeyCountInfo2);
    ResourceUtilizationInfo.setPrimaryKeyCountInfo(primaryKeyCountInfoMap);

    boolean result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, true);
    Assert.assertTrue(result);

    result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, false);
    Assert.assertTrue(result);
  }

  @Test
  public void testNumberOfPrimaryKeysCheckerWithValidRealtimeTableWithoutUpsertDedup() {
    String tableName = "test_REALTIME";

    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.isUpsertEnabled()).thenReturn(false);
    when(mockTableConfig.isDedupEnabled()).thenReturn(false);
    when(_helixResourceManager.getTableConfig(tableName)).thenReturn(mockTableConfig);

    List<String> mockInstances = Arrays.asList("server1", "server2");
    when(_helixResourceManager.getServerInstancesForTable(tableName, TableType.REALTIME)).thenReturn(mockInstances);

    // Mock primary key counts, set count to be higher threshold (to validate that we skip this check for REALTIME
    // tables without upsert / dedup enabled)
    Map<String, PrimaryKeyCountInfo> primaryKeyCountInfoMap = new HashMap<>();
    PrimaryKeyCountInfo primaryKeyCountInfo1 =
        new PrimaryKeyCountInfo("server1", 1000L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server1", primaryKeyCountInfo1);

    PrimaryKeyCountInfo primaryKeyCountInfo2 =
        new PrimaryKeyCountInfo("server2", 2000L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server2", primaryKeyCountInfo2);
    ResourceUtilizationInfo.setPrimaryKeyCountInfo(primaryKeyCountInfoMap);

    boolean result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, true);
    Assert.assertTrue(result);

    result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, false);
    Assert.assertTrue(result);
  }

  @Test
  public void testNumberOfPrimaryKeysCheckerWithValidRealtimeTableWithUpsert() {
    String tableName = "test_REALTIME";

    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.isUpsertEnabled()).thenReturn(true);
    when(mockTableConfig.isDedupEnabled()).thenReturn(false);
    when(_helixResourceManager.getTableConfig(tableName)).thenReturn(mockTableConfig);

    List<String> mockInstances = Arrays.asList("server1", "server2");
    when(_helixResourceManager.getServerInstancesForTable(tableName, TableType.REALTIME)).thenReturn(mockInstances);

    // Mock primary key counts, set below threshold
    Map<String, PrimaryKeyCountInfo> primaryKeyCountInfoMap = new HashMap<>();
    PrimaryKeyCountInfo primaryKeyCountInfo1 =
        new PrimaryKeyCountInfo("server1", 100L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server1", primaryKeyCountInfo1);

    PrimaryKeyCountInfo primaryKeyCountInfo2 =
        new PrimaryKeyCountInfo("server2", 200L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server2", primaryKeyCountInfo2);
    ResourceUtilizationInfo.setPrimaryKeyCountInfo(primaryKeyCountInfoMap);

    boolean result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, true);
    Assert.assertTrue(result);

    result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, false);
    Assert.assertTrue(result);
  }

  @Test
  public void testNumberOfPrimaryKeysCheckerWithValidRealtimeTableWithDedup() {
    String tableName = "test_REALTIME";

    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.isUpsertEnabled()).thenReturn(false);
    when(mockTableConfig.isDedupEnabled()).thenReturn(true);
    when(_helixResourceManager.getTableConfig(tableName)).thenReturn(mockTableConfig);

    List<String> mockInstances = Arrays.asList("server1", "server2");
    when(_helixResourceManager.getServerInstancesForTable(tableName, TableType.REALTIME)).thenReturn(mockInstances);

    // Mock primary key counts, set below threshold
    Map<String, PrimaryKeyCountInfo> primaryKeyCountInfoMap = new HashMap<>();
    PrimaryKeyCountInfo primaryKeyCountInfo1 =
        new PrimaryKeyCountInfo("server1", 100L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server1", primaryKeyCountInfo1);

    PrimaryKeyCountInfo primaryKeyCountInfo2 =
        new PrimaryKeyCountInfo("server2", 200L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server2", primaryKeyCountInfo2);
    ResourceUtilizationInfo.setPrimaryKeyCountInfo(primaryKeyCountInfoMap);

    boolean result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, true);
    Assert.assertTrue(result);

    result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, false);
    Assert.assertTrue(result);
  }

  @Test
  public void testNumberOfPrimaryKeysCheckerWithValidRealtimeTableWithUpsertAboveThreshold() {
    String tableName = "test_REALTIME";

    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.isUpsertEnabled()).thenReturn(true);
    when(mockTableConfig.isDedupEnabled()).thenReturn(false);
    when(_helixResourceManager.getTableConfig(tableName)).thenReturn(mockTableConfig);

    List<String> mockInstances = Arrays.asList("server1", "server2");
    when(_helixResourceManager.getServerInstancesForTable(tableName, TableType.REALTIME)).thenReturn(mockInstances);

    // Mock primary key counts, set at least one count to be higher threshold
    Map<String, PrimaryKeyCountInfo> primaryKeyCountInfoMap = new HashMap<>();
    PrimaryKeyCountInfo primaryKeyCountInfo1 =
        new PrimaryKeyCountInfo("server1", 100L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server1", primaryKeyCountInfo1);

    PrimaryKeyCountInfo primaryKeyCountInfo2 =
        new PrimaryKeyCountInfo("server2", 2000L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server2", primaryKeyCountInfo2);
    ResourceUtilizationInfo.setPrimaryKeyCountInfo(primaryKeyCountInfoMap);

    boolean result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, true);
    Assert.assertFalse(result);

    result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, false);
    Assert.assertTrue(result);
  }

  @Test
  public void testNumberOfPrimaryKeysCheckerWithValidRealtimeTableWithDedupAboveThreshold() {
    String tableName = "test_REALTIME";

    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.isUpsertEnabled()).thenReturn(false);
    when(mockTableConfig.isDedupEnabled()).thenReturn(true);
    when(_helixResourceManager.getTableConfig(tableName)).thenReturn(mockTableConfig);

    List<String> mockInstances = Arrays.asList("server1", "server2");
    when(_helixResourceManager.getServerInstancesForTable(tableName, TableType.REALTIME)).thenReturn(mockInstances);

    // Mock primary key counts, set at least one count to be higher threshold
    Map<String, PrimaryKeyCountInfo> primaryKeyCountInfoMap = new HashMap<>();
    PrimaryKeyCountInfo primaryKeyCountInfo1 =
        new PrimaryKeyCountInfo("server1", 1000L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server1", primaryKeyCountInfo1);

    PrimaryKeyCountInfo primaryKeyCountInfo2 =
        new PrimaryKeyCountInfo("server2", 200L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server2", primaryKeyCountInfo2);
    ResourceUtilizationInfo.setPrimaryKeyCountInfo(primaryKeyCountInfoMap);

    boolean result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, true);
    Assert.assertFalse(result);

    result = _primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, false);
    Assert.assertTrue(result);
  }

  @Test
  public void testNumberOfPrimaryKeysCheckerWithDisabledThresholdValue() {
    PrimaryKeyCountChecker primaryKeyCountChecker;
    ControllerConf controllerConf = mock(ControllerConf.class);

    when(controllerConf.getPrimaryKeyCountThreshold()).thenReturn(-1L);
    when(controllerConf.getPrimaryKeyCountCheckTimeoutMs()).thenReturn(5000);
    when(controllerConf.getResourceUtilizationCheckerFrequency()).thenReturn(120L);

    primaryKeyCountChecker = new PrimaryKeyCountChecker(_helixResourceManager, controllerConf);
    String tableName = "test_REALTIME";

    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.isUpsertEnabled()).thenReturn(false);
    when(mockTableConfig.isDedupEnabled()).thenReturn(true);
    when(_helixResourceManager.getTableConfig(tableName)).thenReturn(mockTableConfig);

    List<String> mockInstances = Arrays.asList("server1", "server2");
    when(_helixResourceManager.getServerInstancesForTable(tableName, TableType.REALTIME)).thenReturn(mockInstances);

    // Mock primary key counts
    Map<String, PrimaryKeyCountInfo> primaryKeyCountInfoMap = new HashMap<>();
    PrimaryKeyCountInfo primaryKeyCountInfo1 =
        new PrimaryKeyCountInfo("server1", 1000L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server1", primaryKeyCountInfo1);

    PrimaryKeyCountInfo primaryKeyCountInfo2 =
        new PrimaryKeyCountInfo("server2", 200L, Set.of("test_REALTIME"), System.currentTimeMillis());
    primaryKeyCountInfoMap.put("server2", primaryKeyCountInfo2);
    ResourceUtilizationInfo.setPrimaryKeyCountInfo(primaryKeyCountInfoMap);

    boolean result = primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, true);
    Assert.assertTrue(result);

    result = primaryKeyCountChecker.isPrimaryKeyCountWithinLimits(tableName, false);
    Assert.assertTrue(result);
  }

  @Test
  public void testComputePrimaryKeyCountValidInstances()
      throws InvalidConfigException {
    Set<String> instances = new HashSet<>(Arrays.asList("server1", "server2"));

    // Mock admin endpoints
    BiMap<String, String> instanceAdminEndpoints = HashBiMap.create();
    instanceAdminEndpoints.put("server1", "http://server1");
    instanceAdminEndpoints.put("server2", "http://server2");
    when(_helixResourceManager.getDataInstanceAdminEndpoints(instances)).thenReturn(instanceAdminEndpoints);

    // Mock responses
    Map<String, String> responseMap = new HashMap<>();
    responseMap.put("http://server1" + PrimaryKeyCountChecker.PRIMARY_KEY_COUNT_API_PATH,
        "{ \"instanceId\": \"server1\", \"numPrimaryKeys\": 42, \"tablesWithPrimaryKeys\": [ \"table_REALTIME\" ] }");
    responseMap.put("http://server2" + PrimaryKeyCountChecker.PRIMARY_KEY_COUNT_API_PATH,
        "{ \"instanceId\": \"server2\", \"numPrimaryKeys\": 2000, \"tablesWithPrimaryKeys\": [ \"table_REALTIME\", "
            + "\"table2_REALTIME\" ], \"lastUpdatedTimeInEpochMs\": 1718668755000 }");

    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        new CompletionServiceHelper.CompletionServiceResponse();
    serviceResponse._httpResponses = responseMap;

    CompletionServiceHelper completionServiceHelper = mock(CompletionServiceHelper.class);
    when(completionServiceHelper.doMultiGetRequest(anyList(), anyString(), anyBoolean(), anyMap(), anyInt(),
        anyString())).thenReturn(serviceResponse);

    // Check the ResourceUtilizationInfo and validate that the counts indicate -1 primary keys
    PrimaryKeyCountInfo primaryKeyCountInfo1 = ResourceUtilizationInfo.getPrimaryKeyCountInfo("server1");
    PrimaryKeyCountInfo primaryKeyCountInfo2 = ResourceUtilizationInfo.getPrimaryKeyCountInfo("server2");

    Assert.assertNotNull(primaryKeyCountInfo1);
    Assert.assertEquals(primaryKeyCountInfo1.getNumPrimaryKeys(), -1L);
    Assert.assertNotNull(primaryKeyCountInfo1.getTablesWithPrimaryKeys());
    Assert.assertEquals(primaryKeyCountInfo1.getTablesWithPrimaryKeys().size(), 0);

    Assert.assertNotNull(primaryKeyCountInfo2);
    Assert.assertEquals(primaryKeyCountInfo2.getNumPrimaryKeys(), -1L);
    Assert.assertNotNull(primaryKeyCountInfo2.getTablesWithPrimaryKeys());
    Assert.assertEquals(primaryKeyCountInfo2.getTablesWithPrimaryKeys().size(), 0);
    Assert.assertEquals(primaryKeyCountInfo2.getLastUpdatedTimeInEpochMs(), -1L);

    // The ResourceUtilizationInfo should be updated after this
    _primaryKeyCountChecker.computePrimaryKeyCount(instanceAdminEndpoints.inverse(), completionServiceHelper);

    // The primary key counts should be updated
    primaryKeyCountInfo1 = ResourceUtilizationInfo.getPrimaryKeyCountInfo("server1");
    primaryKeyCountInfo2 = ResourceUtilizationInfo.getPrimaryKeyCountInfo("server2");

    Assert.assertNotNull(primaryKeyCountInfo1);
    Assert.assertEquals(primaryKeyCountInfo1.getNumPrimaryKeys(), 42L);
    Assert.assertNotNull(primaryKeyCountInfo1.getTablesWithPrimaryKeys());
    Assert.assertEquals(primaryKeyCountInfo1.getTablesWithPrimaryKeys().size(), 1);
    Assert.assertTrue(primaryKeyCountInfo1.getTablesWithPrimaryKeys().contains("table_REALTIME"));

    Assert.assertNotNull(primaryKeyCountInfo2);
    Assert.assertEquals(primaryKeyCountInfo2.getNumPrimaryKeys(), 2000L);
    Assert.assertNotNull(primaryKeyCountInfo2.getTablesWithPrimaryKeys());
    Assert.assertEquals(primaryKeyCountInfo2.getTablesWithPrimaryKeys().size(), 2);
    Assert.assertTrue(primaryKeyCountInfo2.getTablesWithPrimaryKeys().contains("table_REALTIME"));
    Assert.assertTrue(primaryKeyCountInfo2.getTablesWithPrimaryKeys().contains("table2_REALTIME"));
    Assert.assertEquals(primaryKeyCountInfo2.getLastUpdatedTimeInEpochMs(), 1718668755000L);
  }
}

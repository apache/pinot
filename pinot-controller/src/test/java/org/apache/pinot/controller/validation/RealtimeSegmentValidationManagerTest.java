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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.PauseStatusDetails;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.DisasterRecoveryMode;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.config.table.QuotaEnforcementMode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class RealtimeSegmentValidationManagerTest {
  @Mock
  private PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;

  @Mock
  private ResourceUtilizationManager _resourceUtilizationManager;

  @Mock
  private PinotHelixResourceManager _pinotHelixResourceManager;

  @Mock
  private StorageQuotaChecker _storageQuotaChecker;

  @Mock
  private PrimaryKeyQuotaChecker _primaryKeyQuotaChecker;

  @Mock
  private ControllerMetrics _controllerMetrics;

  private AutoCloseable _mocks;
  private RealtimeSegmentValidationManager _realtimeSegmentValidationManager;

  @BeforeMethod
  public void setup() {
    ControllerConf controllerConf = new ControllerConf();
    _mocks = MockitoAnnotations.openMocks(this);
    // By default the primary key quota is not applicable (Mockito returns null enums otherwise); individual tests
    // override these as needed.
    when(_primaryKeyQuotaChecker.getEnforcementMode(any())).thenReturn(QuotaEnforcementMode.DISABLED);
    when(_primaryKeyQuotaChecker.checkPrimaryKeyQuota(any())).thenReturn(PrimaryKeyQuotaChecker.Status.WITHIN_QUOTA);
    _realtimeSegmentValidationManager =
        new RealtimeSegmentValidationManager(controllerConf, _pinotHelixResourceManager, null,
            _llcRealtimeSegmentManager, null, _controllerMetrics, _storageQuotaChecker, _primaryKeyQuotaChecker,
            _resourceUtilizationManager);
  }

  @Test
  public void testReingestionCalledWhenPauselessDisabled() {
    String rawTable = "testTable";
    String tableName = rawTable + "_REALTIME";
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(rawTable)
        .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap()).build();

    // Force shouldEnsureConsuming=false by simulating admin pause
    when(_pinotHelixResourceManager.getTableConfig(tableName)).thenReturn(tableConfig);
    when(_llcRealtimeSegmentManager.getPauseStatusDetails(tableName))
        .thenReturn(new PauseStatusDetails(true, null, PauseState.ReasonCode.ADMINISTRATIVE, null, null));

    _realtimeSegmentValidationManager.processTable(tableName, new RealtimeSegmentValidationManager.Context());

    verify(_llcRealtimeSegmentManager, times(1))
        .repairSegmentsInErrorState(eq(tableConfig), anyBoolean());
  }

  @Test
  public void testNoReingestionWhenPauselessEnabled() {
    String rawTable = "testTable";
    String tableName = rawTable + "_REALTIME";
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(rawTable)
        .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap()).build();

    // Enable pauseless on ingestion config
    org.apache.pinot.spi.config.table.ingestion.IngestionConfig ingestionConfig =
        new org.apache.pinot.spi.config.table.ingestion.IngestionConfig();
    org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig streamIngestionConfig =
        new org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig(
            java.util.List.of(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap()));
    streamIngestionConfig.setPauselessConsumptionEnabled(true);
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    tableConfig.setIngestionConfig(ingestionConfig);

    // Force shouldEnsureConsuming=false by simulating admin pause (to avoid ensureAllPartitionsConsuming)
    when(_pinotHelixResourceManager.getTableConfig(tableName)).thenReturn(tableConfig);
    when(_llcRealtimeSegmentManager.getPauseStatusDetails(tableName))
        .thenReturn(new PauseStatusDetails(true, null, PauseState.ReasonCode.ADMINISTRATIVE, null, null));

    _realtimeSegmentValidationManager.processTable(tableName, new RealtimeSegmentValidationManager.Context());

    verify(_llcRealtimeSegmentManager, times(1))
        .repairSegmentsInErrorState(eq(tableConfig), anyBoolean());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @DataProvider(name = "testCases")
  public Object[][] testCases() {
    return new Object[][]{
        // Table is paused due to admin intervention, should return false
        {true, PauseState.ReasonCode.ADMINISTRATIVE, UtilizationChecker.CheckResult.PASS, false, false},

        // Resource utilization exceeded and pause state is updated, should return false
        {
            false, PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED, UtilizationChecker.CheckResult.FAIL,
            false,
            false
        },

        // Resource utilization is within limits but was previously paused due to resource utilization,
        // should return true
        {
            true, PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED, UtilizationChecker.CheckResult.PASS, false,
            true
        },

        // Resource utilization is STALE but was previously paused due to resource utilization, should return false
        {
            true, PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED,
            UtilizationChecker.CheckResult.UNDETERMINED,
            false, false
        },

        // Resource utilization is STALE but was not previously paused due to resource utilization, should return true
        {
            false, PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED,
            UtilizationChecker.CheckResult.UNDETERMINED,
            false, true
        },

        // Resource utilization is within limits but was previously paused due to storage quota exceeded,
        // should return false
        {true, PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED, UtilizationChecker.CheckResult.PASS, true, false},

        // Storage quota exceeded, should return false
        {false, PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED, UtilizationChecker.CheckResult.PASS, true, false},

        // Storage quota within limits but was previously paused due to storage quota exceeded, should return true
        {true, PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED, UtilizationChecker.CheckResult.PASS, false, true}
    };
  }

  @Test(dataProvider = "testCases")
  public void testShouldEnsureConsuming(boolean isTablePaused, PauseState.ReasonCode reasonCode,
      UtilizationChecker.CheckResult isResourceUtilizationWithinLimits, boolean isQuotaExceeded,
      boolean expectedResult) {
    String tableName = "testTable_REALTIME";
    PauseStatusDetails pauseStatus = mock(PauseStatusDetails.class);
    TableConfig tableConfig = mock(TableConfig.class);

    when(pauseStatus.getPauseFlag()).thenReturn(isTablePaused);
    when(pauseStatus.getReasonCode()).thenReturn(reasonCode);
    when(_llcRealtimeSegmentManager.getPauseStatusDetails(tableName)).thenReturn(pauseStatus);
    when(_resourceUtilizationManager.isResourceUtilizationWithinLimits(tableName,
        UtilizationChecker.CheckPurpose.REALTIME_INGESTION)).thenReturn(isResourceUtilizationWithinLimits);
    when(_pinotHelixResourceManager.getTableConfig(tableName)).thenReturn(tableConfig);
    when(_storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig)).thenReturn(isQuotaExceeded);
    when(_storageQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.ENFORCED);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testConfigChange() {
    Assert.assertEquals(_realtimeSegmentValidationManager.getDisasterRecoveryMode(), DisasterRecoveryMode.DEFAULT);
    Map<String, String> newConfig = new HashMap<>();
    newConfig.put(ControllerConf.ControllerPeriodicTasksConf.DISASTER_RECOVERY_MODE_CONFIG_KEY, "ALWAYS");
    Set<String> changedConfigSet =
        new HashSet<>(List.of(ControllerConf.ControllerPeriodicTasksConf.DISASTER_RECOVERY_MODE_CONFIG_KEY));
    _realtimeSegmentValidationManager.onChange(changedConfigSet, newConfig);
    Assert.assertEquals(_realtimeSegmentValidationManager.getDisasterRecoveryMode(), DisasterRecoveryMode.ALWAYS);
  }

  @Test
  public void testShouldEnsureConsumingInShadowModeDoesNotPause() {
    String tableName = "testTable_REALTIME";
    PauseStatusDetails pauseStatus = mock(PauseStatusDetails.class);
    TableConfig tableConfig = mock(TableConfig.class);

    when(pauseStatus.getPauseFlag()).thenReturn(false);
    when(pauseStatus.getReasonCode()).thenReturn(PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED);
    when(_llcRealtimeSegmentManager.getPauseStatusDetails(tableName)).thenReturn(pauseStatus);
    when(_resourceUtilizationManager.isResourceUtilizationWithinLimits(tableName,
        UtilizationChecker.CheckPurpose.REALTIME_INGESTION)).thenReturn(UtilizationChecker.CheckResult.PASS);
    when(_pinotHelixResourceManager.getTableConfig(tableName)).thenReturn(tableConfig);
    when(_storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig)).thenReturn(true);
    when(_storageQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.SHADOW);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    // quota is breached but enforcement mode is SHADOW, so consumption should not be paused
    Assert.assertTrue(result);
    verify(_llcRealtimeSegmentManager, never()).pauseConsumption(eq(tableName),
        eq(PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED), anyString());
  }

  @Test
  public void testSwitchingToShadowModeResumesQuotaPausedTable() {
    String tableName = "testTable_REALTIME";
    PauseStatusDetails pauseStatus = mock(PauseStatusDetails.class);
    TableConfig tableConfig = mock(TableConfig.class);

    when(pauseStatus.getPauseFlag()).thenReturn(true);
    when(pauseStatus.getReasonCode()).thenReturn(PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED);
    when(_llcRealtimeSegmentManager.getPauseStatusDetails(tableName)).thenReturn(pauseStatus);
    when(_resourceUtilizationManager.isResourceUtilizationWithinLimits(tableName,
        UtilizationChecker.CheckPurpose.REALTIME_INGESTION)).thenReturn(UtilizationChecker.CheckResult.PASS);
    when(_pinotHelixResourceManager.getTableConfig(tableName)).thenReturn(tableConfig);
    when(_storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig)).thenReturn(true);
    when(_storageQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.SHADOW);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertTrue(result);
    verify(_llcRealtimeSegmentManager, never()).pauseConsumption(eq(tableName),
        eq(PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED), anyString());
    verify(_llcRealtimeSegmentManager).updatePauseStateInIdealState(eq(tableName), eq(false),
        eq(PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED), anyString());
  }

  /// Sets up the common mocks for the primary key quota tests: not resource-utilization or storage limited, with the
  /// given current pause state.
  private TableConfig setupForPkTests(String tableName, boolean isTablePaused, PauseState.ReasonCode reasonCode) {
    PauseStatusDetails pauseStatus = mock(PauseStatusDetails.class);
    TableConfig tableConfig = mock(TableConfig.class);
    when(pauseStatus.getPauseFlag()).thenReturn(isTablePaused);
    when(pauseStatus.getReasonCode()).thenReturn(reasonCode);
    when(_llcRealtimeSegmentManager.getPauseStatusDetails(tableName)).thenReturn(pauseStatus);
    when(_resourceUtilizationManager.isResourceUtilizationWithinLimits(tableName,
        UtilizationChecker.CheckPurpose.REALTIME_INGESTION)).thenReturn(UtilizationChecker.CheckResult.PASS);
    when(_pinotHelixResourceManager.getTableConfig(tableName)).thenReturn(tableConfig);
    when(_storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig)).thenReturn(false);
    when(_storageQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.ENFORCED);
    return tableConfig;
  }

  @Test
  public void testPrimaryKeyQuotaExceededPausesConsumption() {
    String tableName = "testTable_REALTIME";
    TableConfig tableConfig = setupForPkTests(tableName, false, null);
    when(_primaryKeyQuotaChecker.checkPrimaryKeyQuota(tableConfig)).thenReturn(PrimaryKeyQuotaChecker.Status.EXCEEDED);
    when(_primaryKeyQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.ENFORCED);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertFalse(result);
    verify(_llcRealtimeSegmentManager).pauseConsumption(eq(tableName),
        eq(PauseState.ReasonCode.PRIMARY_KEY_QUOTA_EXCEEDED), anyString());
  }

  @Test
  public void testPrimaryKeyQuotaShadowModeDoesNotPause() {
    String tableName = "testTable_REALTIME";
    TableConfig tableConfig = setupForPkTests(tableName, false, null);
    when(_primaryKeyQuotaChecker.checkPrimaryKeyQuota(tableConfig)).thenReturn(PrimaryKeyQuotaChecker.Status.EXCEEDED);
    when(_primaryKeyQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.SHADOW);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertTrue(result);
    verify(_llcRealtimeSegmentManager, never()).pauseConsumption(eq(tableName),
        eq(PauseState.ReasonCode.PRIMARY_KEY_QUOTA_EXCEEDED), anyString());
  }

  @Test
  public void testPrimaryKeyQuotaWithinLimitsResumesPkPausedTable() {
    String tableName = "testTable_REALTIME";
    TableConfig tableConfig =
        setupForPkTests(tableName, true, PauseState.ReasonCode.PRIMARY_KEY_QUOTA_EXCEEDED);
    when(_primaryKeyQuotaChecker.checkPrimaryKeyQuota(tableConfig))
        .thenReturn(PrimaryKeyQuotaChecker.Status.WITHIN_QUOTA);
    when(_primaryKeyQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.ENFORCED);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertTrue(result);
    verify(_llcRealtimeSegmentManager).updatePauseStateInIdealState(eq(tableName), eq(false),
        eq(PauseState.ReasonCode.PRIMARY_KEY_QUOTA_EXCEEDED), anyString());
  }

  /// Regression: a table paused for storage quota must NOT be resumed by the primary key check when storage is still
  /// exceeded.
  @Test
  public void testStoragePausedNotResumedByClearPrimaryKey() {
    String tableName = "testTable_REALTIME";
    TableConfig tableConfig = setupForPkTests(tableName, true, PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED);
    // storage still exceeded
    when(_storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig)).thenReturn(true);
    // PK is clear
    when(_primaryKeyQuotaChecker.checkPrimaryKeyQuota(tableConfig))
        .thenReturn(PrimaryKeyQuotaChecker.Status.WITHIN_QUOTA);
    when(_primaryKeyQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.ENFORCED);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertFalse(result);
    verify(_llcRealtimeSegmentManager, never()).updatePauseStateInIdealState(eq(tableName), eq(false),
        any(PauseState.ReasonCode.class), anyString());
  }

  /// Regression for the shared pause-flag bug: a table paused for primary key quota must NOT be resumed by the storage
  /// check when the primary key quota is still exceeded.
  @Test
  public void testPrimaryKeyPausedNotResumedByClearStorage() {
    String tableName = "testTable_REALTIME";
    TableConfig tableConfig = setupForPkTests(tableName, true, PauseState.ReasonCode.PRIMARY_KEY_QUOTA_EXCEEDED);
    // storage clear (default), PK still exceeded
    when(_primaryKeyQuotaChecker.checkPrimaryKeyQuota(tableConfig)).thenReturn(PrimaryKeyQuotaChecker.Status.EXCEEDED);
    when(_primaryKeyQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.ENFORCED);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertFalse(result);
    verify(_llcRealtimeSegmentManager, never()).updatePauseStateInIdealState(eq(tableName), eq(false),
        any(PauseState.ReasonCode.class), anyString());
  }

  /// Both quotas exceeded while paused for primary key: the stored reason is relabeled to storage (which takes
  /// precedence) without a second force-commit, and the table is not resumed.
  @Test
  public void testBothExceededRelabelsToStorageWhilePkPaused() {
    String tableName = "testTable_REALTIME";
    TableConfig tableConfig = setupForPkTests(tableName, true, PauseState.ReasonCode.PRIMARY_KEY_QUOTA_EXCEEDED);
    when(_storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig)).thenReturn(true);
    when(_primaryKeyQuotaChecker.checkPrimaryKeyQuota(tableConfig)).thenReturn(PrimaryKeyQuotaChecker.Status.EXCEEDED);
    when(_primaryKeyQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.ENFORCED);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertFalse(result);
    verify(_llcRealtimeSegmentManager, never()).pauseConsumption(anyString(), any(PauseState.ReasonCode.class),
        anyString());
    verify(_llcRealtimeSegmentManager).updatePauseStateInIdealState(eq(tableName), eq(true),
        eq(PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED), anyString());
  }

  /// A table paused for storage quota whose storage has returned to within limits (or whose storage cap was raised) but
  /// whose primary key quota is still exceeded must stay paused, with the stored reason relabeled from storage to
  /// primary key (no resume, no second force-commit). Symmetric counterpart of
  /// [#testBothExceededRelabelsToStorageWhilePkPaused].
  @Test
  public void testStorageClearedButPrimaryKeyStillExceededRelabelsToPrimaryKey() {
    String tableName = "testTable_REALTIME";
    TableConfig tableConfig = setupForPkTests(tableName, true, PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED);
    // storage now within limits (default from helper), PK still exceeded
    when(_primaryKeyQuotaChecker.checkPrimaryKeyQuota(tableConfig)).thenReturn(PrimaryKeyQuotaChecker.Status.EXCEEDED);
    when(_primaryKeyQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.ENFORCED);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertFalse(result);
    // Not resumed...
    verify(_llcRealtimeSegmentManager, never()).updatePauseStateInIdealState(eq(tableName), eq(false),
        any(PauseState.ReasonCode.class), anyString());
    // ...no fresh pause (already paused)...
    verify(_llcRealtimeSegmentManager, never()).pauseConsumption(anyString(), any(PauseState.ReasonCode.class),
        anyString());
    // ...just relabeled to PRIMARY_KEY_QUOTA_EXCEEDED while staying paused.
    verify(_llcRealtimeSegmentManager).updatePauseStateInIdealState(eq(tableName), eq(true),
        eq(PauseState.ReasonCode.PRIMARY_KEY_QUOTA_EXCEEDED), anyString());
  }

  /// A PK-paused table whose primary key status cannot be determined (e.g. servers unreachable) is left paused.
  @Test
  public void testPrimaryKeyUndeterminedLeavesPkPausedTablePaused() {
    String tableName = "testTable_REALTIME";
    TableConfig tableConfig = setupForPkTests(tableName, true, PauseState.ReasonCode.PRIMARY_KEY_QUOTA_EXCEEDED);
    when(_primaryKeyQuotaChecker.checkPrimaryKeyQuota(tableConfig))
        .thenReturn(PrimaryKeyQuotaChecker.Status.UNDETERMINED);
    when(_primaryKeyQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.ENFORCED);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertFalse(result);
    verify(_llcRealtimeSegmentManager, never()).updatePauseStateInIdealState(eq(tableName), eq(false),
        any(PauseState.ReasonCode.class), anyString());
  }

  /// A storage-paused table whose storage is back within limits stays paused while an enforced primary key quota cannot
  /// be determined, since it may still be exceeded. Resuming would only re-pause it (and force-commit again) once the
  /// counts are readable.
  @Test
  public void testPrimaryKeyUndeterminedLeavesStoragePausedTablePaused() {
    String tableName = "testTable_REALTIME";
    TableConfig tableConfig = setupForPkTests(tableName, true, PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED);
    // storage now within limits (default from helper), PK undetermined
    when(_primaryKeyQuotaChecker.checkPrimaryKeyQuota(tableConfig))
        .thenReturn(PrimaryKeyQuotaChecker.Status.UNDETERMINED);
    when(_primaryKeyQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.ENFORCED);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertFalse(result);
    verify(_llcRealtimeSegmentManager, never()).updatePauseStateInIdealState(eq(tableName), anyBoolean(),
        any(PauseState.ReasonCode.class), anyString());
  }

  /// In SHADOW mode the primary key quota never holds consumption, so an undetermined status does not keep a table
  /// paused, e.g. after the mode was switched from ENFORCED to SHADOW to release a primary-key-paused table.
  @Test
  public void testPrimaryKeyUndeterminedInShadowModeResumesPkPausedTable() {
    String tableName = "testTable_REALTIME";
    TableConfig tableConfig = setupForPkTests(tableName, true, PauseState.ReasonCode.PRIMARY_KEY_QUOTA_EXCEEDED);
    when(_primaryKeyQuotaChecker.checkPrimaryKeyQuota(tableConfig))
        .thenReturn(PrimaryKeyQuotaChecker.Status.UNDETERMINED);
    when(_primaryKeyQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.SHADOW);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertTrue(result);
    verify(_llcRealtimeSegmentManager).updatePauseStateInIdealState(eq(tableName), eq(false),
        eq(PauseState.ReasonCode.PRIMARY_KEY_QUOTA_EXCEEDED), anyString());
  }

  /// An undetermined primary key quota never pauses a table that is consuming; only a breach does.
  @Test
  public void testPrimaryKeyUndeterminedDoesNotPauseConsumingTable() {
    String tableName = "testTable_REALTIME";
    TableConfig tableConfig = setupForPkTests(tableName, false, null);
    when(_primaryKeyQuotaChecker.checkPrimaryKeyQuota(tableConfig))
        .thenReturn(PrimaryKeyQuotaChecker.Status.UNDETERMINED);
    when(_primaryKeyQuotaChecker.getEnforcementMode(tableConfig)).thenReturn(QuotaEnforcementMode.ENFORCED);

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertTrue(result);
    verify(_llcRealtimeSegmentManager, never()).pauseConsumption(anyString(), any(PauseState.ReasonCode.class),
        anyString());
  }
}

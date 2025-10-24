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

import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.PauseStatusDetails;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.PauseState;
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
  private ControllerMetrics _controllerMetrics;

  private AutoCloseable _mocks;
  private RealtimeSegmentValidationManager _realtimeSegmentValidationManager;

  @BeforeMethod
  public void setup() {
    ControllerConf controllerConf = new ControllerConf();
    _mocks = MockitoAnnotations.openMocks(this);
    _realtimeSegmentValidationManager =
        new RealtimeSegmentValidationManager(controllerConf, _pinotHelixResourceManager, null,
            _llcRealtimeSegmentManager, null, _controllerMetrics, _storageQuotaChecker, _resourceUtilizationManager);
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

    boolean result = _realtimeSegmentValidationManager.shouldEnsureConsuming(tableName);

    Assert.assertEquals(result, expectedResult);
  }
}

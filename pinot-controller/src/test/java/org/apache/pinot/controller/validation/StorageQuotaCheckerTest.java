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

import java.util.Map;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.QuotaEnforcementMode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class StorageQuotaCheckerTest {
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  // Separate table for enforcement mode tests: the metrics registry is shared, and other tests in this class assert
  // that gauges for OFFLINE_TABLE_NAME do not exist yet
  private static final String MODE_TEST_TABLE_NAME = "modeTestTable_OFFLINE";
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String SEGMENT_NAME = "testSegment";
  private static final long SEGMENT_SIZE_IN_BYTES = 1024;
  private static final int NUM_REPLICAS = 2;

  private TableSizeReader _tableSizeReader;
  private StorageQuotaChecker _storageQuotaChecker;
  private ControllerConf _controllerConf;

  @BeforeClass
  public void init() {
    _controllerConf = mock(ControllerConf.class);
    when(_controllerConf.getServerAdminRequestTimeoutSeconds()).thenReturn(1);
    when(_controllerConf.getEnableStorageQuotaCheck()).thenReturn(true);
  }

  @Test
  public void testNoQuota()
      throws InvalidConfigException {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME).setNumReplicas(NUM_REPLICAS).build();
    _tableSizeReader = mock(TableSizeReader.class);
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _storageQuotaChecker = new StorageQuotaChecker(_tableSizeReader, controllerMetrics,
        mock(LeadControllerManager.class), mock(PinotHelixResourceManager.class), _controllerConf);
    tableConfig.setQuotaConfig(null);
    assertTrue(isSegmentWithinQuota(tableConfig));
  }

  @Test
  public void testDimensionTable()
      throws InvalidConfigException {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setIsDimTable(true).setTableName(OFFLINE_TABLE_NAME)
            .setNumReplicas(NUM_REPLICAS).build();
    _tableSizeReader = mock(TableSizeReader.class);
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _storageQuotaChecker = new StorageQuotaChecker(_tableSizeReader, controllerMetrics,
        mock(LeadControllerManager.class), mock(PinotHelixResourceManager.class), _controllerConf);
    tableConfig.setQuotaConfig(null);
    assertTrue(isSegmentWithinQuota(tableConfig));
  }

  @Test
  public void testRealtimeTable()
      throws InvalidConfigException {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(REALTIME_TABLE_NAME)
        .setNumReplicas(NUM_REPLICAS).build();
    _tableSizeReader = mock(TableSizeReader.class);
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(pinotHelixResourceManager.getNumReplicas(eq(tableConfig))).thenReturn(NUM_REPLICAS);
    _storageQuotaChecker =
        new StorageQuotaChecker(_tableSizeReader, controllerMetrics, mock(LeadControllerManager.class),
            pinotHelixResourceManager, _controllerConf);

    tableConfig.setQuotaConfig(new QuotaConfig(null, null));
    assertFalse(_storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig));

    tableConfig.setQuotaConfig(new QuotaConfig("2.8K", null));

    // Within quota but with missing segments, should pass without updating metrics
    mockTableSizeResult(REALTIME_TABLE_NAME, 4 * 1024, 1);
    assertFalse(_storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig));


    // Exceed quota and with missing segments, should fail without updating metrics
    mockTableSizeResult(REALTIME_TABLE_NAME, 8 * 1024, 1);
    assertTrue(_storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig));
  }

  @Test
  public void testNoStorageQuotaConfig()
      throws InvalidConfigException {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME).setNumReplicas(NUM_REPLICAS).build();
    _tableSizeReader = mock(TableSizeReader.class);
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _storageQuotaChecker = new StorageQuotaChecker(_tableSizeReader, controllerMetrics,
        mock(LeadControllerManager.class), mock(PinotHelixResourceManager.class), _controllerConf);
    tableConfig.setQuotaConfig(new QuotaConfig(null, null));
    assertTrue(isSegmentWithinQuota(tableConfig));
  }

  @Test
  public void testWithinQuota()
      throws InvalidConfigException {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME).setNumReplicas(NUM_REPLICAS).build();
    _tableSizeReader = mock(TableSizeReader.class);
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(pinotHelixResourceManager.getNumReplicas(eq(tableConfig))).thenReturn(NUM_REPLICAS);
    _storageQuotaChecker =
        new StorageQuotaChecker(_tableSizeReader, controllerMetrics, mock(LeadControllerManager.class),
            pinotHelixResourceManager, _controllerConf);
    tableConfig.setQuotaConfig(new QuotaConfig("2.8K", null));

    // No response from server, should pass without updating metrics
    mockTableSizeResult(OFFLINE_TABLE_NAME, -1, 0);
    assertTrue(isSegmentWithinQuota(tableConfig));
    assertFalse(
        MetricValueUtils.tableGaugeExists(controllerMetrics, OFFLINE_TABLE_NAME,
            ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE));

    // Within quota but with missing segments, should pass without updating metrics
    mockTableSizeResult(OFFLINE_TABLE_NAME, 4 * 1024, 1);
    assertTrue(isSegmentWithinQuota(tableConfig));
    assertFalse(
        MetricValueUtils.tableGaugeExists(controllerMetrics, OFFLINE_TABLE_NAME,
            ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE));


    // Exceed quota and with missing segments, should fail without updating metrics
    mockTableSizeResult(OFFLINE_TABLE_NAME, 8 * 1024, 1);
    assertFalse(isSegmentWithinQuota(tableConfig));
    assertFalse(
        MetricValueUtils.tableGaugeExists(controllerMetrics, OFFLINE_TABLE_NAME,
            ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE));

    // Within quota without missing segments, should pass and update metrics
    mockTableSizeResult(OFFLINE_TABLE_NAME, 3 * 1024, 0);
    assertTrue(isSegmentWithinQuota(tableConfig));
    assertEquals(
        MetricValueUtils.getTableGaugeValue(controllerMetrics, OFFLINE_TABLE_NAME,
            ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE), 3 * 1024);

    // Exceed quota without missing segments, should fail and update metrics
    mockTableSizeResult(OFFLINE_TABLE_NAME, 4 * 1024, 0);
    assertFalse(isSegmentWithinQuota(tableConfig));
    assertEquals(
        MetricValueUtils.getTableGaugeValue(controllerMetrics, OFFLINE_TABLE_NAME,
            ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE), 4 * 1024);

    // Exceed quota but refreshing segment with equal or smaller size, should pass and update metrics
    mockTableSizeResult(OFFLINE_TABLE_NAME, 4 * 1024, 0);
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(SEGMENT_NAME);
    segmentZKMetadata.setSizeInBytes(SEGMENT_SIZE_IN_BYTES);
    when(pinotHelixResourceManager.getSegmentZKMetadata(
        TableNameBuilder.forType(tableConfig.getTableType()).tableNameWithType(tableConfig.getTableName()),
        SEGMENT_NAME)).thenReturn(segmentZKMetadata);
    assertTrue(isSegmentWithinQuota(tableConfig));
    assertEquals(
        MetricValueUtils.getTableGaugeValue(controllerMetrics, OFFLINE_TABLE_NAME,
            ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE), 4 * 1024);
  }

  private StorageQuotaChecker newCheckerWithBreachedTable(TableConfig tableConfig, ControllerMetrics controllerMetrics)
      throws InvalidConfigException {
    _tableSizeReader = mock(TableSizeReader.class);
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(pinotHelixResourceManager.getNumReplicas(eq(tableConfig))).thenReturn(NUM_REPLICAS);
    // allowed = 2.8K * 2 replicas = 5.6K; estimated 4K + incoming 1K * 2 = 6K -> breach
    mockTableSizeResult(MODE_TEST_TABLE_NAME, 4 * 1024, 0);
    return new StorageQuotaChecker(_tableSizeReader, controllerMetrics, mock(LeadControllerManager.class),
        pinotHelixResourceManager, _controllerConf);
  }

  private static long breachCount(ControllerMetrics controllerMetrics) {
    return controllerMetrics.getMeteredTableValue(MODE_TEST_TABLE_NAME, ControllerMeter.STORAGE_QUOTA_EXCEEDED)
        .count();
  }

  @Test
  public void testShadowModeEmitsBreachMeterOncePerBreach()
      throws InvalidConfigException {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(MODE_TEST_TABLE_NAME)
        .setNumReplicas(NUM_REPLICAS).build();
    QuotaConfig quotaConfig = new QuotaConfig("2.8K", null, QuotaEnforcementMode.SHADOW, null);
    tableConfig.setQuotaConfig(quotaConfig);
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _storageQuotaChecker = newCheckerWithBreachedTable(tableConfig, controllerMetrics);

    long before = breachCount(controllerMetrics);
    // The checker still reports the true quota state so that callers/metrics see the breach
    assertFalse(isSegmentWithinQuota(tableConfig));
    assertEquals(breachCount(controllerMetrics), before + 1);
    assertFalse(isSegmentWithinQuota(tableConfig));
    assertEquals(breachCount(controllerMetrics), before + 2);

    // The table-level check used by the realtime validation manager also reports the true state and meters it
    mockTableSizeResult(MODE_TEST_TABLE_NAME, 8 * 1024, 0);
    assertTrue(_storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig));
    assertEquals(breachCount(controllerMetrics), before + 3);
  }

  @Test
  public void testEnforcedModeEmitsSameBreachMeterAsShadow()
      throws InvalidConfigException {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(MODE_TEST_TABLE_NAME)
        .setNumReplicas(NUM_REPLICAS).build();
    QuotaConfig quotaConfig = new QuotaConfig("2.8K", null, QuotaEnforcementMode.ENFORCED, null);
    tableConfig.setQuotaConfig(quotaConfig);
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _storageQuotaChecker = newCheckerWithBreachedTable(tableConfig, controllerMetrics);

    long before = breachCount(controllerMetrics);
    assertFalse(isSegmentWithinQuota(tableConfig));
    assertEquals(breachCount(controllerMetrics), before + 1);
    assertFalse(isSegmentWithinQuota(tableConfig));
    assertEquals(breachCount(controllerMetrics), before + 2);
  }

  @Test
  public void testDisabledModeSkipsTheCheck()
      throws InvalidConfigException {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(MODE_TEST_TABLE_NAME)
        .setNumReplicas(NUM_REPLICAS).build();
    QuotaConfig quotaConfig = new QuotaConfig("2.8K", null, QuotaEnforcementMode.DISABLED, null);
    tableConfig.setQuotaConfig(quotaConfig);
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _storageQuotaChecker = newCheckerWithBreachedTable(tableConfig, controllerMetrics);

    long before = breachCount(controllerMetrics);
    assertTrue(isSegmentWithinQuota(tableConfig));
    assertFalse(_storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig));
    assertEquals(breachCount(controllerMetrics), before);
    // Table sizes are not even fetched from servers when the check is disabled
    verify(_tableSizeReader, never()).getTableSubtypeSize(anyString(), anyInt(), anyBoolean());
  }

  @Test
  public void testEnforcementModeResolution() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME).setNumReplicas(NUM_REPLICAS).build();
    _storageQuotaChecker =
        new StorageQuotaChecker(mock(TableSizeReader.class),
            new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), mock(LeadControllerManager.class),
            mock(PinotHelixResourceManager.class), _controllerConf);

    // No quota config: defaults to ENFORCED
    tableConfig.setQuotaConfig(null);
    assertEquals(_storageQuotaChecker.getEnforcementMode(tableConfig), QuotaEnforcementMode.ENFORCED);

    // Quota config without a mode: defaults to ENFORCED
    tableConfig.setQuotaConfig(new QuotaConfig("10G", null));
    assertEquals(_storageQuotaChecker.getEnforcementMode(tableConfig), QuotaEnforcementMode.ENFORCED);

    // The table's own mode is used
    tableConfig.setQuotaConfig(new QuotaConfig("10G", null, QuotaEnforcementMode.SHADOW, null));
    assertEquals(_storageQuotaChecker.getEnforcementMode(tableConfig), QuotaEnforcementMode.SHADOW);

    // The pre-existing controller-wide switch (controller.enable.storage.quota.check=false) disables every table
    ControllerConf disabledConf = mock(ControllerConf.class);
    when(disabledConf.getServerAdminRequestTimeoutSeconds()).thenReturn(1);
    when(disabledConf.getEnableStorageQuotaCheck()).thenReturn(false);
    StorageQuotaChecker disabledChecker =
        new StorageQuotaChecker(mock(TableSizeReader.class),
            new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), mock(LeadControllerManager.class),
            mock(PinotHelixResourceManager.class), disabledConf);
    assertEquals(disabledChecker.getEnforcementMode(tableConfig), QuotaEnforcementMode.DISABLED);
    tableConfig.setQuotaConfig(new QuotaConfig("10G", null, QuotaEnforcementMode.ENFORCED, null));
    assertEquals(disabledChecker.getEnforcementMode(tableConfig), QuotaEnforcementMode.DISABLED);
  }

  private boolean isSegmentWithinQuota(TableConfig tableConfig)
      throws InvalidConfigException {
    return _storageQuotaChecker
        .isSegmentStorageWithinQuota(tableConfig, SEGMENT_NAME, SEGMENT_SIZE_IN_BYTES, SEGMENT_SIZE_IN_BYTES)
        ._isSegmentWithinQuota;
  }

  public void mockTableSizeResult(String tableName, long tableSizeInBytes, int numMissingSegments)
      throws InvalidConfigException {
    TableSizeReader.TableSubTypeSizeDetails tableSizeResult = new TableSizeReader.TableSubTypeSizeDetails();
    tableSizeResult._estimatedSizeInBytes = tableSizeInBytes;
    tableSizeResult._segments = Map.of();
    tableSizeResult._missingSegments = numMissingSegments;
    when(_tableSizeReader.getTableSubtypeSize(tableName, 1000, true)).thenReturn(tableSizeResult);
  }
}

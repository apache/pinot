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

import java.util.Collections;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class StorageQuotaCheckerTest {
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
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
  }

  private boolean isSegmentWithinQuota(TableConfig tableConfig)
      throws InvalidConfigException {
    return _storageQuotaChecker
        .isSegmentStorageWithinQuota(tableConfig, SEGMENT_NAME, SEGMENT_SIZE_IN_BYTES)._isSegmentWithinQuota;
  }

  public void mockTableSizeResult(String tableName, long tableSizeInBytes, int numMissingSegments)
      throws InvalidConfigException {
    TableSizeReader.TableSubTypeSizeDetails tableSizeResult = new TableSizeReader.TableSubTypeSizeDetails();
    tableSizeResult._estimatedSizeInBytes = tableSizeInBytes;
    tableSizeResult._segments = Collections.emptyMap();
    tableSizeResult._missingSegments = numMissingSegments;
    when(_tableSizeReader.getTableSubtypeSize(tableName, 1000)).thenReturn(tableSizeResult);
  }
}

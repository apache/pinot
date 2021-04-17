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
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class StorageQuotaCheckerTest {
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static final String SEGMENT_NAME = "testSegment";
  private static final long SEGMENT_SIZE_IN_BYTES = 1024;
  private static final int NUM_REPLICAS = 2;

  private TableSizeReader _tableSizeReader;
  private TableConfig _tableConfig;
  private ControllerMetrics _controllerMetrics;
  private StorageQuotaChecker _storageQuotaChecker;

  @BeforeClass
  public void setUp() {
    _tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME).setNumReplicas(NUM_REPLICAS).build();
    _tableSizeReader = mock(TableSizeReader.class);
    _controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _storageQuotaChecker = new StorageQuotaChecker(_tableConfig, _tableSizeReader, _controllerMetrics, true);
  }

  private boolean isSegmentWithinQuota() throws InvalidConfigException {
    return _storageQuotaChecker.isSegmentStorageWithinQuota(SEGMENT_NAME, SEGMENT_SIZE_IN_BYTES,
        1000).isSegmentWithinQuota;
  }

  @Test
  public void testNoQuota() throws InvalidConfigException {
    _tableConfig.setQuotaConfig(null);
    assertTrue(isSegmentWithinQuota());
  }

  @Test
  public void testNoStorageQuotaConfig() throws InvalidConfigException {
    _tableConfig.setQuotaConfig(new QuotaConfig(null, null));
    assertTrue(isSegmentWithinQuota());
  }

  public void mockTableSizeResult(long tableSizeInBytes, int numMissingSegments) throws InvalidConfigException {
    TableSizeReader.TableSubTypeSizeDetails tableSizeResult = new TableSizeReader.TableSubTypeSizeDetails();
    tableSizeResult.estimatedSizeInBytes = tableSizeInBytes;
    tableSizeResult.segments = Collections.emptyMap();
    tableSizeResult.missingSegments = numMissingSegments;
    when(_tableSizeReader.getTableSubtypeSize(OFFLINE_TABLE_NAME, 1000)).thenReturn(tableSizeResult);
  }

  @Test
  public void testWithinQuota() throws InvalidConfigException {
    _tableConfig.setQuotaConfig(new QuotaConfig("2.8K", null));

    // No response from server, should pass without updating metrics
    mockTableSizeResult(-1, 0);
    assertTrue(isSegmentWithinQuota());
    assertEquals(
        _controllerMetrics.getValueOfTableGauge(OFFLINE_TABLE_NAME, ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE), 0);

    // Within quota but with missing segments, should pass without updating metrics
    mockTableSizeResult(4 * 1024, 1);
    assertTrue(isSegmentWithinQuota());
    assertEquals(
        _controllerMetrics.getValueOfTableGauge(OFFLINE_TABLE_NAME, ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE), 0);

    // Exceed quota and with missing segments, should fail without updating metrics
    mockTableSizeResult(8 * 1024, 1);
    assertFalse(isSegmentWithinQuota());
    assertEquals(
        _controllerMetrics.getValueOfTableGauge(OFFLINE_TABLE_NAME, ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE), 0);

    // Within quota without missing segments, should pass and update metrics
    mockTableSizeResult(3 * 1024, 0);
    assertTrue(isSegmentWithinQuota());
    assertEquals(
        _controllerMetrics.getValueOfTableGauge(OFFLINE_TABLE_NAME, ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE),
        3 * 1024);

    // Exceed quota without missing segments, should fail and update metrics
    mockTableSizeResult(4 * 1024, 0);
    assertFalse(isSegmentWithinQuota());
    assertEquals(
        _controllerMetrics.getValueOfTableGauge(OFFLINE_TABLE_NAME, ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE),
        4 * 1024);
  }
}

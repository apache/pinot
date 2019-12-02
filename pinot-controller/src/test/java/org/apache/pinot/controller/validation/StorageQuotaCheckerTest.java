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

import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.config.QuotaConfig;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.util.TableSizeReader;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class StorageQuotaCheckerTest {
  private TableSizeReader _tableSizeReader;
  private TableConfig _tableConfig;
  private ControllerMetrics _controllerMetrics;
  private boolean _isLeaderForTable;
  private QuotaConfig _quotaConfig;
  private SegmentsValidationAndRetentionConfig _validationConfig;
  private static final File TEST_DIR = new File(StorageQuotaCheckerTest.class.getName());

  @BeforeClass
  public void setUp() {
    _tableSizeReader = mock(TableSizeReader.class);
    _tableConfig = mock(TableConfig.class);
    _quotaConfig = mock(QuotaConfig.class);
    _controllerMetrics = new ControllerMetrics(new MetricsRegistry());
    _validationConfig = mock(SegmentsValidationAndRetentionConfig.class);
    _isLeaderForTable = true;
    when(_tableConfig.getValidationConfig()).thenReturn(_validationConfig);
    when(_validationConfig.getReplicationNumber()).thenReturn(2);
    TEST_DIR.mkdirs();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEST_DIR);
  }

  @Test
  public void testNoQuota()
      throws InvalidConfigException {
    StorageQuotaChecker checker =
        new MockStorageQuotaChecker(_tableConfig, _tableSizeReader, _controllerMetrics, _isLeaderForTable);
    when(_tableConfig.getQuotaConfig()).thenReturn(null);
    StorageQuotaChecker.QuotaCheckerResponse res = checker.isSegmentStorageWithinQuota(TEST_DIR, "segment", 1000);
    Assert.assertTrue(res.isSegmentWithinQuota);
  }

  @Test
  public void testNoStorageQuotaConfig()
      throws InvalidConfigException {
    StorageQuotaChecker checker =
        new MockStorageQuotaChecker(_tableConfig, _tableSizeReader, _controllerMetrics, _isLeaderForTable);
    when(_tableConfig.getQuotaConfig()).thenReturn(_quotaConfig);
    when(_quotaConfig.storageSizeBytes()).thenReturn(-1L);
    StorageQuotaChecker.QuotaCheckerResponse res = checker.isSegmentStorageWithinQuota(TEST_DIR, "segment", 1000);
    Assert.assertTrue(res.isSegmentWithinQuota);
  }

  public void setupTableSegmentSize(final long tableSize, final long segmentSize, final int missing)
      throws InvalidConfigException {
    when(_tableSizeReader.getTableSubtypeSize("testTable", 1000))
        .thenAnswer(new Answer<TableSizeReader.TableSubTypeSizeDetails>() {
          @Override
          public TableSizeReader.TableSubTypeSizeDetails answer(InvocationOnMock invocationOnMock)
              throws Throwable {
            TableSizeReader.TableSubTypeSizeDetails sizeDetails = new TableSizeReader.TableSubTypeSizeDetails();
            sizeDetails.estimatedSizeInBytes = tableSize;
            TableSizeReader.SegmentSizeDetails segSizeDetails = new TableSizeReader.SegmentSizeDetails();
            segSizeDetails.estimatedSizeInBytes = segmentSize;
            sizeDetails.segments.put("segment1", segSizeDetails);
            sizeDetails.missingSegments = missing;
            return sizeDetails;
          }
        });
  }

  @Test
  public void testWithinQuota()
      throws IOException, InvalidConfigException {
    File tempFile = new File(TEST_DIR, "small_file");
    tempFile.createNewFile();
    byte[] data = new byte[1024];
    Arrays.fill(data, (byte) 1);
    try (FileOutputStream ostr = new FileOutputStream(tempFile)) {
      ostr.write(data);
    }
    String tableName = "testTable";
    setupTableSegmentSize(4800L, 900L, 0);
    when(_tableConfig.getTableName()).thenReturn(tableName);
    when(_tableConfig.getQuotaConfig()).thenReturn(_quotaConfig);
    when(_quotaConfig.storageSizeBytes()).thenReturn(3000L);
    when(_quotaConfig.getStorage()).thenReturn("3K");
    StorageQuotaChecker checker =
        new MockStorageQuotaChecker(_tableConfig, _tableSizeReader, _controllerMetrics, _isLeaderForTable);
    StorageQuotaChecker.QuotaCheckerResponse response = checker.isSegmentStorageWithinQuota(TEST_DIR, "segment1", 1000);
    Assert.assertTrue(response.isSegmentWithinQuota);
    Assert.assertEquals(
        _controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.TABLE_STORAGE_QUOTA_UTILIZATION), 80L);

    // Quota exceeded.
    when(_quotaConfig.storageSizeBytes()).thenReturn(2800L);
    when(_quotaConfig.getStorage()).thenReturn("2.8K");
    response = checker.isSegmentStorageWithinQuota(TEST_DIR, "segment1", 1000);
    Assert.assertFalse(response.isSegmentWithinQuota);
    Assert.assertEquals(
        _controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.TABLE_STORAGE_QUOTA_UTILIZATION), 85L);

    // Table already over quota.
    setupTableSegmentSize(6000L, 900L, 0);
    when(_quotaConfig.storageSizeBytes()).thenReturn(2800L);
    when(_quotaConfig.getStorage()).thenReturn("2.8K");
    response = checker.isSegmentStorageWithinQuota(TEST_DIR, "segment1", 1000);
    Assert.assertFalse(response.isSegmentWithinQuota);
    Assert.assertEquals(
        _controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.TABLE_STORAGE_QUOTA_UTILIZATION), 107L);

    // no response from any server
    setupTableSegmentSize(-1, -1, 0);
    when(_quotaConfig.storageSizeBytes()).thenReturn(2800L);
    when(_quotaConfig.getStorage()).thenReturn("2.8K");
    response = checker.isSegmentStorageWithinQuota(TEST_DIR, "segment1", 1000);
    Assert.assertTrue(response.isSegmentWithinQuota);

    // partial response from servers, but table already over quota
    setupTableSegmentSize(6000L, 900L, -2);
    when(_quotaConfig.storageSizeBytes()).thenReturn(2800L);
    when(_quotaConfig.getStorage()).thenReturn("2.8K");
    response = checker.isSegmentStorageWithinQuota(TEST_DIR, "segment1", 1000);
    Assert.assertFalse(response.isSegmentWithinQuota);

    // partial response from servers, but current estimate within quota
    setupTableSegmentSize(2000L, 900L, -2);
    when(_quotaConfig.storageSizeBytes()).thenReturn(2800L);
    when(_quotaConfig.getStorage()).thenReturn("2.8K");
    response = checker.isSegmentStorageWithinQuota(TEST_DIR, "segment1", 1000);
    Assert.assertTrue(response.isSegmentWithinQuota);
  }

  private class MockStorageQuotaChecker extends StorageQuotaChecker {

    public MockStorageQuotaChecker(TableConfig tableConfig, TableSizeReader tableSizeReader,
        ControllerMetrics controllerMetrics, boolean isLeaderForTable) {
      super(tableConfig, tableSizeReader, controllerMetrics, isLeaderForTable);
    }
  }
}

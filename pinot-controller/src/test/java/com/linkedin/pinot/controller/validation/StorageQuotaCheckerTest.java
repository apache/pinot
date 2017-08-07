/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.controller.validation;

import com.linkedin.pinot.common.config.QuotaConfig;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.controller.util.TableSizeReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class StorageQuotaCheckerTest {
  TableSizeReader tableSizeReader;
  TableConfig tableConfig;
  private QuotaConfig quotaConfig;
  private SegmentsValidationAndRetentionConfig validationConfig;
  private final File TEST_DIR = new File(StorageQuotaCheckerTest.class.getName());

  @BeforeClass
  public void setUp() {
    tableSizeReader = mock(TableSizeReader.class);
    tableConfig = mock(TableConfig.class);
    quotaConfig = mock(QuotaConfig.class);
    validationConfig = mock(SegmentsValidationAndRetentionConfig.class);
    when(tableConfig.getValidationConfig()).thenReturn(validationConfig);
    when(validationConfig.getReplicationNumber()).thenReturn(2);
    TEST_DIR.mkdirs();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEST_DIR);
  }

  @Test
  public void testNoQuota() {
    StorageQuotaChecker checker = new StorageQuotaChecker(tableConfig, tableSizeReader);
    when(tableConfig.getQuotaConfig()).thenReturn(null);
    StorageQuotaChecker.QuotaCheckerResponse res =
        checker.isSegmentStorageWithinQuota(TEST_DIR, "myTable", "segment", 1000);
    Assert.assertTrue(res.isSegmentWithinQuota);
  }

  @Test
  public void testNoStorageQuotaConfig() {
    StorageQuotaChecker checker = new StorageQuotaChecker(tableConfig, tableSizeReader);
    when(tableConfig.getQuotaConfig()).thenReturn(quotaConfig);
    when(quotaConfig.storageSizeBytes()).thenReturn(-1L);
    StorageQuotaChecker.QuotaCheckerResponse res =
        checker.isSegmentStorageWithinQuota(TEST_DIR, "myTable", "segment", 1000);
    Assert.assertTrue(res.isSegmentWithinQuota);
  }

  public void setupTableSegmentSize(final long tableSize, final long segmentSize) {
    when(tableSizeReader.getTableSubtypeSize("testTable", 1000)).thenAnswer(
        new Answer<TableSizeReader.TableSubTypeSizeDetails>() {
          @Override
          public TableSizeReader.TableSubTypeSizeDetails answer(InvocationOnMock invocationOnMock)
              throws Throwable {
            TableSizeReader.TableSubTypeSizeDetails sizeDetails = new TableSizeReader.TableSubTypeSizeDetails();
            sizeDetails.estimatedSizeInBytes = tableSize;
            TableSizeReader.SegmentSizeDetails segSizeDetails = new TableSizeReader.SegmentSizeDetails();
            segSizeDetails.estimatedSizeInBytes = segmentSize;
            sizeDetails.segments.put("segment1", segSizeDetails);
            return sizeDetails;
          }
        });
  }

  @Test
  public void testWithinQuota()
      throws IOException {
    File tempFile = new File(TEST_DIR, "small_file");
    tempFile.createNewFile();
    byte[] data = new byte[1024];
    Arrays.fill(data, (byte) 1);
    try (FileOutputStream ostr = new FileOutputStream(tempFile)) {
      ostr.write(data);
    }
    setupTableSegmentSize(5800, 900);
    when(tableConfig.getQuotaConfig()).thenReturn(quotaConfig);
    when(quotaConfig.storageSizeBytes()).thenReturn(3000L);
    StorageQuotaChecker checker = new StorageQuotaChecker(tableConfig, tableSizeReader);
    StorageQuotaChecker.QuotaCheckerResponse response =
        checker.isSegmentStorageWithinQuota(TEST_DIR, "testTable", "segment1", 1000);
    Assert.assertTrue(response.isSegmentWithinQuota);

    when(quotaConfig.storageSizeBytes()).thenReturn(2800L);
    response = checker.isSegmentStorageWithinQuota(TEST_DIR, "testTable", "segment1", 1000);
    Assert.assertFalse(response.isSegmentWithinQuota);
  }
}

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
package org.apache.pinot.segment.local.segment.index.loader;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class LoaderUtilsTest {
  private static final File TEST_DIR = new File(FileUtils.getTempDirectory(), LoaderUtils.class.getName());

  @BeforeClass
  public void setUp() throws IOException {
    FileUtils.deleteDirectory(TEST_DIR);
    Assert.assertTrue(TEST_DIR.mkdirs());
  }

  @Test
  public void testReloadFailureRecovery() throws IOException {
    String segmentName = "dummySegment";
    String indexFileName = "dummyIndex";
    File indexDir = new File(TEST_DIR, segmentName);
    File segmentBackupDir = new File(TEST_DIR, segmentName + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    File segmentTempDir = new File(TEST_DIR, segmentName + CommonConstants.Segment.SEGMENT_TEMP_DIR_SUFFIX);

    // Only index directory exists (normal case, or failed before the first renaming)
    Assert.assertTrue(indexDir.mkdir());
    FileUtils.touch(new File(indexDir, indexFileName));
    LoaderUtils.reloadFailureRecovery(indexDir);
    Assert.assertTrue(indexDir.exists());
    Assert.assertTrue(new File(indexDir, indexFileName).exists());
    Assert.assertFalse(segmentBackupDir.exists());
    Assert.assertFalse(segmentTempDir.exists());
    FileUtils.deleteDirectory(indexDir);

    // Only segment backup directory exists (failed after the first renaming but before copying happened)
    Assert.assertTrue(segmentBackupDir.mkdir());
    FileUtils.touch(new File(segmentBackupDir, indexFileName));
    LoaderUtils.reloadFailureRecovery(indexDir);
    Assert.assertTrue(indexDir.exists());
    Assert.assertTrue(new File(indexDir, indexFileName).exists());
    Assert.assertFalse(segmentBackupDir.exists());
    Assert.assertFalse(segmentTempDir.exists());
    FileUtils.deleteDirectory(indexDir);

    // Index directory and segment backup directory exist (failed before second renaming)
    Assert.assertTrue(indexDir.mkdir());
    Assert.assertTrue(segmentBackupDir.mkdir());
    FileUtils.touch(new File(segmentBackupDir, indexFileName));
    LoaderUtils.reloadFailureRecovery(indexDir);
    Assert.assertTrue(indexDir.exists());
    Assert.assertTrue(new File(indexDir, indexFileName).exists());
    Assert.assertFalse(segmentBackupDir.exists());
    Assert.assertFalse(segmentTempDir.exists());
    FileUtils.deleteDirectory(indexDir);

    // Index directory and segment temporary directory exist (failed after second renaming)
    Assert.assertTrue(indexDir.mkdir());
    FileUtils.touch(new File(indexDir, indexFileName));
    Assert.assertTrue(segmentTempDir.mkdir());
    LoaderUtils.reloadFailureRecovery(indexDir);
    Assert.assertTrue(indexDir.exists());
    Assert.assertTrue(new File(indexDir, indexFileName).exists());
    Assert.assertFalse(segmentBackupDir.exists());
    Assert.assertFalse(segmentTempDir.exists());
    FileUtils.deleteDirectory(indexDir);
  }

  @AfterClass
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(TEST_DIR);
  }
}

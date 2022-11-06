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
package org.apache.pinot.segment.local.loader;

import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;


public class TierBasedSegmentDirectoryLoaderTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "TierBasedSegmentDirectoryLoaderTest");
  private static final String TABLE_NAME = "table01";
  private static final String TABLE_NAME_WITH_TYPE = "table01_OFFLINE";

  @BeforeMethod
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testDeleteSegmentOnDefaultTier()
      throws Exception {
    TierBasedSegmentDirectoryLoader loader = new TierBasedSegmentDirectoryLoader();
    SegmentDirectoryLoaderContext loaderCtx = new SegmentDirectoryLoaderContext.Builder().setSegmentName("seg01")
        .setTableDataDir(TEMP_DIR.getAbsolutePath() + "/" + TABLE_NAME_WITH_TYPE).build();
    // When segDir is on the default tier.
    File tableDataDir = new File(TEMP_DIR.getAbsolutePath(), TABLE_NAME_WITH_TYPE);
    File segDataDir = new File(tableDataDir, "seg01");
    FileUtils.touch(segDataDir);
    loader.delete(loaderCtx);
    assertFalse(segDataDir.exists());
  }

  @Test
  public void testDeleteSegmentOnLastKnownTier()
      throws Exception {
    String tierName = "tier01";
    TierBasedSegmentDirectoryLoader loader = new TierBasedSegmentDirectoryLoader();
    SegmentDirectoryLoaderContext loaderCtx = new SegmentDirectoryLoaderContext.Builder().setSegmentName("seg01")
        .setTableDataDir(TEMP_DIR.getAbsolutePath() + "/" + TABLE_NAME_WITH_TYPE).build();

    // When segDir is under the tier data path.
    File tableDataDir = new File(new File(TEMP_DIR, tierName), TABLE_NAME_WITH_TYPE);
    File segDataDir = new File(tableDataDir, "seg01");
    FileUtils.touch(segDataDir);

    // Put the tier name and tier data path into the tier track file.
    File tierTrackFile = new File(TEMP_DIR.getAbsolutePath() + "/" + TABLE_NAME_WITH_TYPE, "seg01.tier");
    TierBasedSegmentDirectoryLoader.writeTo(tierTrackFile, tierName, segDataDir.getAbsolutePath());
    loader.delete(loaderCtx);
    assertFalse(segDataDir.exists());
    assertFalse(tierTrackFile.exists());
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*unexpected version.*")
  public void testDeleteSegmentBadTrackFile()
      throws Exception {
    TierBasedSegmentDirectoryLoader loader = new TierBasedSegmentDirectoryLoader();
    SegmentDirectoryLoaderContext loaderCtx = new SegmentDirectoryLoaderContext.Builder().setSegmentName("seg01")
        .setTableDataDir(TEMP_DIR.getAbsolutePath() + "/" + TABLE_NAME_WITH_TYPE).build();
    // Corrupt the tier track file.
    File tierTrackFile = new File(TEMP_DIR.getAbsolutePath() + "/" + TABLE_NAME_WITH_TYPE, "seg01.tier");
    FileUtils.write(tierTrackFile, "a lot of bad data", StandardCharsets.UTF_8);
    loader.delete(loaderCtx);
  }
}

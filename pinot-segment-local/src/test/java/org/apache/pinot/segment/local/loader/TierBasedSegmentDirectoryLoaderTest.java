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
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
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
  public void testDropSegmentOnDefaultTier()
      throws Exception {
    TableConfig tableCfg = mock(TableConfig.class);
    TierBasedSegmentDirectoryLoader loader = new TierBasedSegmentDirectoryLoader();
    SegmentDirectoryLoaderContext loaderCtx =
        new SegmentDirectoryLoaderContext.Builder().setTableConfig(tableCfg).setSegmentName("seg01")
            .setTableDataDir(TEMP_DIR.getAbsolutePath() + "/" + TABLE_NAME_WITH_TYPE).build();
    // When segDir is on the default tier.
    File tableDataDir = new File(TEMP_DIR.getAbsolutePath(), TABLE_NAME_WITH_TYPE);
    File segDataDir = new File(tableDataDir, "seg01");
    FileUtils.touch(segDataDir);
    loader.drop(loaderCtx);
    assertFalse(segDataDir.exists());
  }

  @Test
  public void testDropSegmentOnLastKnownTier()
      throws Exception {
    String tierName = "tier01";
    TableConfig tableCfg = createTableConfigWithTier(tierName, new File(TEMP_DIR, tierName));
    TierBasedSegmentDirectoryLoader loader = new TierBasedSegmentDirectoryLoader();
    SegmentDirectoryLoaderContext loaderCtx =
        new SegmentDirectoryLoaderContext.Builder().setTableConfig(tableCfg).setSegmentName("seg01")
            .setTableDataDir(TEMP_DIR.getAbsolutePath() + "/" + TABLE_NAME_WITH_TYPE).build();

    // When segDir is on a specific tier.
    // Put the tier name into the tier track file.
    File tierTrackFile = new File(TEMP_DIR.getAbsolutePath() + "/" + TABLE_NAME_WITH_TYPE, "seg01.tier");
    FileUtils.writeStringToFile(tierTrackFile, tierName, StandardCharsets.UTF_8);
    File tableDataDir = new File(new File(TEMP_DIR, tierName), TABLE_NAME_WITH_TYPE);
    File segDataDir = new File(tableDataDir, "seg01");
    FileUtils.touch(segDataDir);
    loader.drop(loaderCtx);
    assertFalse(segDataDir.exists());
    assertFalse(tierTrackFile.exists());
  }

  @Test
  public void testDropSegmentOnTargetTier()
      throws Exception {
    String tierName = "tier02";
    TableConfig tableCfg = createTableConfigWithTier(tierName, new File(TEMP_DIR, tierName));
    TierBasedSegmentDirectoryLoader loader = new TierBasedSegmentDirectoryLoader();
    SegmentDirectoryLoaderContext loaderCtx =
        new SegmentDirectoryLoaderContext.Builder().setTableConfig(tableCfg).setSegmentName("seg01")
            .setSegmentTier(tierName).setTableDataDir(TEMP_DIR.getAbsolutePath() + "/" + TABLE_NAME_WITH_TYPE).build();

    // When segDir is on a specific tier.
    File tableDataDir = new File(new File(TEMP_DIR, tierName), TABLE_NAME_WITH_TYPE);
    File segDataDir = new File(tableDataDir, "seg01");
    FileUtils.touch(segDataDir);
    loader.drop(loaderCtx);
    assertFalse(segDataDir.exists());
  }

  private TableConfig createTableConfigWithTier(String tierName, File dataDir) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(Collections
        .singletonList(new TierConfig(tierName, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "3d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tag_OFFLINE", null,
            Collections.singletonMap("dataDir", dataDir.getAbsolutePath())))).build();
  }
}

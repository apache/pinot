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
package org.apache.pinot.core.data.manager;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.data.manager.TableDataManagerParams;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.crypt.PinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.fetcher.BaseSegmentFetcher.RETRY_COUNT_CONFIG_KEY;
import static org.apache.pinot.common.utils.fetcher.BaseSegmentFetcher.RETRY_DELAY_SCALE_FACTOR_CONFIG_KEY;
import static org.apache.pinot.common.utils.fetcher.BaseSegmentFetcher.RETRY_WAIT_MS_CONFIG_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class BaseTableDataManagerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "BaseTableDataManagerTest");
  private static final String TABLE_NAME = "table01";
  private static final String TABLE_NAME_WITH_TYPE = "table01_OFFLINE";
  private static final File TABLE_DATA_DIR = new File(TEMP_DIR, TABLE_NAME_WITH_TYPE);
  private static final String STRING_COLUMN = "col1";
  private static final String[] STRING_VALUES = {"A", "D", "E", "B", "C"};
  private static final String LONG_COLUMN = "col2";
  private static final long[] LONG_VALUES = {10000L, 20000L, 50000L, 40000L, 30000L};

  @BeforeMethod
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    initSegmentFetcher();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testReloadSegmentNewData()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    SegmentZKMetadata zkmd = createRawSegment(tableConfig, segName, SegmentVersion.v3, 5);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    SegmentMetadata llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn("0");

    BaseTableDataManager tmgr = createTableManager();
    assertFalse(tmgr.getSegmentDataDir(segName).exists());
    tmgr.reloadSegment(segName, createIndexLoadingConfig(), zkmd, llmd, null, false);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());
    llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getTotalDocs(), 5);
  }

  @Test
  public void testReloadSegmentNewDataNewTier()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    SegmentZKMetadata zkmd = createRawSegment(tableConfig, segName, SegmentVersion.v3, 5);
    String tierName = "coolTier";
    when(zkmd.getTier()).thenReturn(tierName);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    SegmentMetadata llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn("0");

    // No dataDir for coolTier, thus stay on default tier.
    BaseTableDataManager tmgr = createTableManager();
    File defaultSegDir = tmgr.getSegmentDataDir(segName);
    assertFalse(defaultSegDir.exists());
    tmgr.reloadSegment(segName, createIndexLoadingConfig("tierBased", tableConfig), zkmd, llmd, null, false);
    assertTrue(defaultSegDir.exists());
    llmd = new SegmentMetadataImpl(defaultSegDir);
    assertEquals(llmd.getTotalDocs(), 5);

    // Configured dataDir for coolTier, thus move to new dir.
    llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn("0");
    tableConfig = createTableConfigWithTier(tierName, new File(TEMP_DIR, tierName));
    tmgr = createTableManager();
    IndexLoadingConfig loadingCfg = createIndexLoadingConfig("tierBased", tableConfig);
    tmgr.reloadSegment(segName, loadingCfg, zkmd, llmd, null, false);
    File segDirOnTier = tmgr.getSegmentDataDir(segName, tierName, loadingCfg.getTableConfig());
    assertTrue(segDirOnTier.exists());
    assertFalse(defaultSegDir.exists());
    llmd = new SegmentMetadataImpl(segDirOnTier);
    assertEquals(llmd.getTotalDocs(), 5);
    assertEquals(llmd.getIndexDir(), segDirOnTier);
  }

  @Test
  public void testReloadSegmentUseLocalCopy()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    File localSegDir = createSegment(tableConfig, segName, SegmentVersion.v1, 5);
    String segCrc = getCRC(localSegDir, SegmentVersion.v1);

    // Same CRCs so load the local segment directory directly.
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getCrc()).thenReturn(Long.valueOf(segCrc));
    SegmentMetadata llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn(segCrc);

    BaseTableDataManager tmgr = createTableManager();
    tmgr.reloadSegment(segName, createIndexLoadingConfig(), zkmd, llmd, null, false);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());
    llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getTotalDocs(), 5);

    FileUtils.deleteQuietly(localSegDir);
    try {
      tmgr.reloadSegment(segName, createIndexLoadingConfig(), zkmd, llmd, null, false);
      fail();
    } catch (Exception e) {
      // As expected, segment reloading fails due to missing the local segment dir.
      assertTrue(e.getMessage().contains("does not exist or is not a directory"));
    }
  }

  @Test
  public void testReloadSegmentUseLocalCopyNewTier()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    File localSegDir = createSegment(tableConfig, segName, SegmentVersion.v1, 5);
    String segCrc = getCRC(localSegDir, SegmentVersion.v1);

    // Same CRCs so load the local segment directory directly.
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getCrc()).thenReturn(Long.valueOf(segCrc));
    String tierName = "coolTier";
    when(zkmd.getTier()).thenReturn(tierName);
    SegmentMetadata llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn(segCrc);

    // No dataDir for coolTier, thus stay on default tier.
    BaseTableDataManager tmgr = createTableManager();
    tmgr.reloadSegment(segName, createIndexLoadingConfig("tierBased", tableConfig), zkmd, llmd, null, false);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());
    llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getTotalDocs(), 5);
    assertEquals(llmd.getIndexDir(), localSegDir);

    // Configured dataDir for coolTier, thus move to new dir.
    llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn(segCrc);
    tableConfig = createTableConfigWithTier(tierName, new File(TEMP_DIR, tierName));
    tmgr = createTableManager();
    IndexLoadingConfig loadingCfg = createIndexLoadingConfig("tierBased", tableConfig);
    tmgr.reloadSegment(segName, loadingCfg, zkmd, llmd, null, false);
    File segDirOnTier = tmgr.getSegmentDataDir(segName, tierName, loadingCfg.getTableConfig());
    assertTrue(segDirOnTier.exists());
    assertFalse(localSegDir.exists());
    llmd = new SegmentMetadataImpl(segDirOnTier);
    assertEquals(llmd.getTotalDocs(), 5);
    assertEquals(llmd.getIndexDir(), segDirOnTier);
  }

  @Test
  public void testReloadSegmentConvertVersion()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    File localSegDir = createSegment(tableConfig, segName, SegmentVersion.v1, 5);
    String segCrc = getCRC(localSegDir, SegmentVersion.v1);

    // Same CRCs so load the local segment directory directly.
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getCrc()).thenReturn(Long.valueOf(segCrc));
    SegmentMetadata llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn(segCrc);

    // Require to use v3 format.
    IndexLoadingConfig idxCfg = createIndexLoadingConfig();
    idxCfg.setSegmentVersion(SegmentVersion.v3);

    BaseTableDataManager tmgr = createTableManager();
    tmgr.reloadSegment(segName, idxCfg, zkmd, llmd, null, false);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());
    llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getVersion(), SegmentVersion.v3);
    assertEquals(llmd.getTotalDocs(), 5);
  }

  @Test
  public void testReloadSegmentAddIndex()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    File localSegDir = createSegment(tableConfig, segName, SegmentVersion.v3, 5);
    String segCrc = getCRC(localSegDir, SegmentVersion.v3);
    assertFalse(hasInvertedIndex(localSegDir, STRING_COLUMN, SegmentVersion.v3));
    assertFalse(hasInvertedIndex(localSegDir, LONG_COLUMN, SegmentVersion.v3));

    // Same CRCs so load the local segment directory directly.
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getCrc()).thenReturn(Long.valueOf(segCrc));
    SegmentMetadata llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn(segCrc);

    // Require to add indices.
    IndexLoadingConfig idxCfg = createIndexLoadingConfig();
    idxCfg.setSegmentVersion(SegmentVersion.v3);
    idxCfg.setInvertedIndexColumns(new HashSet<>(Arrays.asList(STRING_COLUMN, LONG_COLUMN)));

    BaseTableDataManager tmgr = createTableManager();
    tmgr.reloadSegment(segName, idxCfg, zkmd, llmd, null, false);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());
    llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getTotalDocs(), 5);
    assertTrue(hasInvertedIndex(tmgr.getSegmentDataDir(segName), STRING_COLUMN, SegmentVersion.v3));
    assertTrue(hasInvertedIndex(tmgr.getSegmentDataDir(segName), LONG_COLUMN, SegmentVersion.v3));
  }

  @Test
  public void testReloadSegmentForceDownload()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    SegmentZKMetadata zkmd = createRawSegment(tableConfig, segName, SegmentVersion.v3, 5);
    File localSegDir = createSegment(tableConfig, segName, SegmentVersion.v3, 5);

    // Same CRC but force to download.
    BaseTableDataManager tmgr = createTableManager();
    SegmentMetadataImpl llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getCrc(), zkmd.getCrc() + "");

    // Remove the local segment dir. Segment reloading fails unless force to download.
    FileUtils.deleteQuietly(localSegDir);
    try {
      tmgr.reloadSegment(segName, createIndexLoadingConfig(), zkmd, llmd, null, false);
      fail();
    } catch (Exception e) {
      // As expected, segment reloading fails due to missing the local segment dir.
      assertTrue(e.getMessage().contains("does not exist or is not a directory"));
    }

    tmgr.reloadSegment(segName, createIndexLoadingConfig(), zkmd, llmd, null, true);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());

    llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getCrc(), zkmd.getCrc() + "");
    assertEquals(llmd.getTotalDocs(), 5);
  }

  @Test
  public void testAddOrReplaceSegmentNewData()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    SegmentZKMetadata zkmd = createRawSegment(tableConfig, segName, SegmentVersion.v3, 5);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    SegmentMetadata llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn("0");

    BaseTableDataManager tmgr = createTableManager();
    assertFalse(tmgr.getSegmentDataDir(segName).exists());
    tmgr.addOrReplaceSegment(segName, createIndexLoadingConfig(), zkmd, llmd);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());
    llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getTotalDocs(), 5);
  }

  @Test
  public void testAddOrReplaceSegmentNewDataNewTier()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    SegmentZKMetadata zkmd = createRawSegment(tableConfig, segName, SegmentVersion.v3, 5);
    String tierName = "coolTier";
    when(zkmd.getTier()).thenReturn(tierName);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    SegmentMetadata llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn("0");

    // No dataDir for coolTier, thus stay on default tier.
    BaseTableDataManager tmgr = createTableManager();
    File defaultSegDir = tmgr.getSegmentDataDir(segName);
    assertFalse(defaultSegDir.exists());
    tmgr.addOrReplaceSegment(segName, createIndexLoadingConfig("tierBased", tableConfig), zkmd, llmd);
    assertTrue(defaultSegDir.exists());
    llmd = new SegmentMetadataImpl(defaultSegDir);
    assertEquals(llmd.getTotalDocs(), 5);

    // Configured dataDir for coolTier, thus move to new dir.
    llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn("0");
    tableConfig = createTableConfigWithTier(tierName, new File(TEMP_DIR, tierName));
    tmgr = createTableManager();
    IndexLoadingConfig loadingCfg = createIndexLoadingConfig("tierBased", tableConfig);
    tmgr.addOrReplaceSegment(segName, loadingCfg, zkmd, llmd);
    File segDirOnTier = tmgr.getSegmentDataDir(segName, tierName, loadingCfg.getTableConfig());
    assertTrue(segDirOnTier.exists());
    assertFalse(defaultSegDir.exists());
    llmd = new SegmentMetadataImpl(segDirOnTier);
    assertEquals(llmd.getTotalDocs(), 5);
    assertEquals(llmd.getIndexDir(), segDirOnTier);
  }

  @Test
  public void testAddOrReplaceSegmentNoop()
      throws Exception {
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getCrc()).thenReturn(Long.valueOf(1024));

    SegmentMetadata llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn("1024");

    BaseTableDataManager tmgr = createTableManager();
    assertFalse(tmgr.getSegmentDataDir("seg01").exists());
    tmgr.addOrReplaceSegment("seg01", createIndexLoadingConfig(), zkmd, llmd);
    // As CRC is same, the index dir is left as is, so not get created by the test.
    assertFalse(tmgr.getSegmentDataDir("seg01").exists());
  }

  @Test
  public void testAddOrReplaceSegmentUseLocalCopy()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    File localSegDir = createSegment(tableConfig, segName, SegmentVersion.v3, 5);
    String segCrc = getCRC(localSegDir, SegmentVersion.v3);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getCrc()).thenReturn(Long.valueOf(segCrc));
    when(zkmd.getDownloadUrl()).thenReturn("file://somewhere");

    BaseTableDataManager tmgr = createTableManager();
    tmgr.addOrReplaceSegment(segName, createIndexLoadingConfig(), zkmd, null);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());
    SegmentMetadataImpl llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getTotalDocs(), 5);

    FileUtils.deleteQuietly(localSegDir);
    try {
      tmgr.addOrReplaceSegment(segName, createIndexLoadingConfig(), zkmd, null);
      fail();
    } catch (Exception e) {
      // As expected, when local segment dir is missing, it tries to download
      // raw segment from deep store, but it would fail with bad download uri.
      assertEquals(e.getMessage(), "Operation failed after 3 attempts");
    }
  }

  @Test
  public void testAddOrReplaceSegmentUseLocalCopyNewTier()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    File localSegDir = createSegment(tableConfig, segName, SegmentVersion.v3, 5);
    String segCrc = getCRC(localSegDir, SegmentVersion.v3);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getCrc()).thenReturn(Long.valueOf(segCrc));
    when(zkmd.getDownloadUrl()).thenReturn("file://somewhere");
    String tierName = "coolTier";
    when(zkmd.getTier()).thenReturn(tierName);

    // No dataDir for coolTier, thus stay on default tier.
    BaseTableDataManager tmgr = createTableManager();
    tmgr.addOrReplaceSegment(segName, createIndexLoadingConfig("tierBased", tableConfig), zkmd, null);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());
    SegmentMetadataImpl llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getTotalDocs(), 5);
    assertEquals(llmd.getIndexDir(), localSegDir);

    // Configured dataDir for coolTier, thus move to new dir.
    tableConfig = createTableConfigWithTier(tierName, new File(TEMP_DIR, tierName));
    IndexLoadingConfig loadingCfg = createIndexLoadingConfig("tierBased", tableConfig);
    File segDirOnTier = tmgr.getSegmentDataDir(segName, tierName, loadingCfg.getTableConfig());
    assertFalse(segDirOnTier.exists());
    // Move segDir to new tier to see if addOrReplaceSegment() can load segDir from new tier directly.
    FileUtils.moveDirectory(localSegDir, segDirOnTier);
    tmgr = createTableManager();
    tmgr.addOrReplaceSegment(segName, loadingCfg, zkmd, null);
    llmd = new SegmentMetadataImpl(segDirOnTier);
    assertEquals(llmd.getTotalDocs(), 5);
    assertEquals(llmd.getIndexDir(), segDirOnTier);
  }

  @Test
  public void testAddOrReplaceSegmentUseBackupCopy()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    File localSegDir = createSegment(tableConfig, segName, SegmentVersion.v3, 5);
    String segCrc = getCRC(localSegDir, SegmentVersion.v3);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getCrc()).thenReturn(Long.valueOf(segCrc));

    BaseTableDataManager tmgr = createTableManager();
    File backup = tmgr.getSegmentDataDir(segName + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    localSegDir.renameTo(backup);

    assertFalse(tmgr.getSegmentDataDir(segName).exists());
    tmgr.addOrReplaceSegment(segName, createIndexLoadingConfig(), zkmd, null);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());
    SegmentMetadataImpl llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getTotalDocs(), 5);
  }

  @Test
  public void testAddOrReplaceSegmentStaleBackupCopy()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    SegmentZKMetadata zkmd = createRawSegment(tableConfig, segName, SegmentVersion.v3, 5);

    BaseTableDataManager tmgr = createTableManager();
    // Create a local segment with fewer rows, making its CRC different from the raw segment.
    // So that the raw segment is downloaded and loaded in the end.
    File localSegDir = createSegment(tableConfig, segName, SegmentVersion.v3, 3);
    File backup = tmgr.getSegmentDataDir(segName + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    localSegDir.renameTo(backup);

    assertFalse(tmgr.getSegmentDataDir(segName).exists());
    tmgr.addOrReplaceSegment(segName, createIndexLoadingConfig(), zkmd, null);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());
    SegmentMetadataImpl llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getTotalDocs(), 5);
  }

  @Test
  public void testAddOrReplaceSegmentUpConvertVersion()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    File localSegDir = createSegment(tableConfig, segName, SegmentVersion.v1, 5);
    String segCrc = getCRC(localSegDir, SegmentVersion.v1);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getCrc()).thenReturn(Long.valueOf(segCrc));

    // Require to use v3 format.
    IndexLoadingConfig idxCfg = createIndexLoadingConfig();
    idxCfg.setSegmentVersion(SegmentVersion.v3);

    BaseTableDataManager tmgr = createTableManager();
    tmgr.addOrReplaceSegment(segName, idxCfg, zkmd, null);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());
    SegmentMetadataImpl llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getVersion(), SegmentVersion.v3);
    assertEquals(llmd.getTotalDocs(), 5);
  }

  @Test
  public void testAddOrReplaceSegmentDownConvertVersion()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    File localSegDir = createSegment(tableConfig, segName, SegmentVersion.v3, 5);
    String segCrc = getCRC(localSegDir, SegmentVersion.v3);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getCrc()).thenReturn(Long.valueOf(segCrc));

    // Require to use v1 format.
    IndexLoadingConfig idxCfg = createIndexLoadingConfig();
    idxCfg.setSegmentVersion(SegmentVersion.v1);

    BaseTableDataManager tmgr = createTableManager();
    tmgr.addOrReplaceSegment(segName, idxCfg, zkmd, null);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());
    SegmentMetadataImpl llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    // The existing segment preprocessing logic doesn't down convert segment format.
    assertEquals(llmd.getVersion(), SegmentVersion.v3);
    assertEquals(llmd.getTotalDocs(), 5);
  }

  @Test
  public void testAddOrReplaceSegmentAddIndex()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    String segName = "seg01";
    File localSegDir = createSegment(tableConfig, segName, SegmentVersion.v3, 5);
    String segCrc = getCRC(localSegDir, SegmentVersion.v3);
    assertFalse(hasInvertedIndex(localSegDir, STRING_COLUMN, SegmentVersion.v3));
    assertFalse(hasInvertedIndex(localSegDir, LONG_COLUMN, SegmentVersion.v3));

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getCrc()).thenReturn(Long.valueOf(segCrc));

    // Require to add indices.
    IndexLoadingConfig idxCfg = createIndexLoadingConfig();
    idxCfg.setSegmentVersion(SegmentVersion.v3);
    idxCfg.setInvertedIndexColumns(new HashSet<>(Arrays.asList(STRING_COLUMN, LONG_COLUMN)));

    BaseTableDataManager tmgr = createTableManager();
    tmgr.addOrReplaceSegment(segName, idxCfg, zkmd, null);
    assertTrue(tmgr.getSegmentDataDir(segName).exists());
    SegmentMetadataImpl llmd = new SegmentMetadataImpl(tmgr.getSegmentDataDir(segName));
    assertEquals(llmd.getTotalDocs(), 5);
    assertTrue(hasInvertedIndex(tmgr.getSegmentDataDir(segName), STRING_COLUMN, SegmentVersion.v3));
    assertTrue(hasInvertedIndex(tmgr.getSegmentDataDir(segName), LONG_COLUMN, SegmentVersion.v3));
  }

  @Test
  public void testDownloadAndDecrypt()
      throws Exception {
    File tempInput = new File(TEMP_DIR, "tmp.txt");
    FileUtils.write(tempInput, "this is from somewhere remote");

    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getDownloadUrl()).thenReturn("file://" + tempInput.getAbsolutePath());

    BaseTableDataManager tmgr = createTableManager();
    File tempRootDir = tmgr.getTmpSegmentDataDir("test-download-decrypt");

    File tarFile = tmgr.downloadAndDecrypt("seg01", zkmd, tempRootDir);
    assertEquals(FileUtils.readFileToString(tarFile), "this is from somewhere remote");

    when(zkmd.getCrypterName()).thenReturn("fakePinotCrypter");
    tarFile = tmgr.downloadAndDecrypt("seg01", zkmd, tempRootDir);
    assertEquals(FileUtils.readFileToString(tarFile), "this is from somewhere remote");

    FakePinotCrypter fakeCrypter = (FakePinotCrypter) PinotCrypterFactory.create("fakePinotCrypter");
    String parentDir = TABLE_NAME_WITH_TYPE + "/tmp/test-download-decrypt/";
    assertTrue(fakeCrypter._origFile.getAbsolutePath().endsWith(parentDir + "seg01.tar.gz.enc"));
    assertTrue(fakeCrypter._decFile.getAbsolutePath().endsWith(parentDir + "seg01.tar.gz"));

    try {
      // Set maxRetry to 0 to cause retry failure immediately.
      Map<String, Object> properties = new HashMap<>();
      properties.put(RETRY_COUNT_CONFIG_KEY, 0);
      SegmentFetcherFactory.init(new PinotConfiguration(properties));
      tmgr.downloadAndDecrypt("seg01", zkmd, tempRootDir);
      fail();
    } catch (AttemptsExceededException e) {
      assertEquals(e.getMessage(), "Operation failed after 0 attempts");
    }
  }

  @Test
  public void testUntarAndMoveSegment()
      throws IOException {
    BaseTableDataManager tmgr = createTableManager();
    File tempRootDir = tmgr.getTmpSegmentDataDir("test-untar-move");

    // All input and intermediate files are put in the tempRootDir.
    File tempTar = new File(tempRootDir, "seg01" + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    File tempInputDir = new File(tempRootDir, "seg01_input");
    FileUtils.write(new File(tempInputDir, "tmp.txt"), "this is in segment dir");
    TarGzCompressionUtils.createTarGzFile(tempInputDir, tempTar);
    FileUtils.deleteQuietly(tempInputDir);

    // The destination is the segment directory at the same level of tempRootDir.
    File indexDir = tmgr.untarAndMoveSegment("seg01", tempTar, tempRootDir);
    assertEquals(indexDir, tmgr.getSegmentDataDir("seg01"));
    assertEquals(FileUtils.readFileToString(new File(indexDir, "tmp.txt")), "this is in segment dir");

    try {
      tmgr.untarAndMoveSegment("seg01", new File(tempRootDir, "unknown.txt"), TEMP_DIR);
      fail();
    } catch (Exception e) {
      // expected.
    }
  }

  // Has to be public class for the class loader to work.
  public static class FakePinotCrypter implements PinotCrypter {
    private File _origFile;
    private File _decFile;

    @Override
    public void init(PinotConfiguration config) {
    }

    @Override
    public void encrypt(File origFile, File encFile) {
    }

    @Override
    public void decrypt(File origFile, File decFile) {
      _origFile = origFile;
      _decFile = decFile;
    }
  }

  private static void initSegmentFetcher()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(RETRY_COUNT_CONFIG_KEY, 3);
    properties.put(RETRY_WAIT_MS_CONFIG_KEY, 100);
    properties.put(RETRY_DELAY_SCALE_FACTOR_CONFIG_KEY, 5);
    SegmentFetcherFactory.init(new PinotConfiguration(properties));

    // Setup crypter
    properties.put("class.fakePinotCrypter", FakePinotCrypter.class.getName());
    PinotCrypterFactory.init(new PinotConfiguration(properties));
  }

  private static IndexLoadingConfig createIndexLoadingConfig() {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    return indexLoadingConfig;
  }

  private static IndexLoadingConfig createIndexLoadingConfig(String segDirLoader, TableConfig tableConfig) {
    InstanceDataManagerConfig idmc = mock(InstanceDataManagerConfig.class);
    when(idmc.getSegmentDirectoryLoader()).thenReturn(segDirLoader);
    when(idmc.getConfig()).thenReturn(new PinotConfiguration());
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(idmc, tableConfig);
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    return indexLoadingConfig;
  }

  private static BaseTableDataManager createTableManager() {
    TableDataManagerConfig config = mock(TableDataManagerConfig.class);
    when(config.getTableName()).thenReturn(TABLE_NAME_WITH_TYPE);
    when(config.getDataDir()).thenReturn(TABLE_DATA_DIR.getAbsolutePath());
    when(config.getAuthConfig()).thenReturn(new MapConfiguration(Collections.emptyMap()));

    OfflineTableDataManager tableDataManager = new OfflineTableDataManager();
    tableDataManager.init(config, "dummyInstance", mock(ZkHelixPropertyStore.class),
        new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), mock(HelixManager.class), null,
        new TableDataManagerParams(0, false, -1));
    tableDataManager.start();
    return tableDataManager;
  }

  private static SegmentZKMetadata createRawSegment(TableConfig tableConfig, String segName, SegmentVersion segVer,
      int rowCnt)
      throws Exception {
    File segDir = createSegment(tableConfig, segName, segVer, rowCnt);
    String segCrc = getCRC(segDir, SegmentVersion.v3);

    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    File tempTar = new File(TEMP_DIR, segName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarGzCompressionUtils.createTarGzFile(segDir, tempTar);
    when(zkmd.getDownloadUrl()).thenReturn("file://" + tempTar.getAbsolutePath());
    when(zkmd.getCrc()).thenReturn(Long.valueOf(segCrc));

    FileUtils.deleteQuietly(segDir);
    return zkmd;
  }

  private static File createSegment(TableConfig tableConfig, String segName, SegmentVersion segVer, int rowCnt)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
        .addMetric(LONG_COLUMN, FieldSpec.DataType.LONG).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TABLE_DATA_DIR.getAbsolutePath());
    config.setSegmentName(segName);
    config.setSegmentVersion(segVer);
    List<GenericRow> rows = new ArrayList<>(3);
    for (int i = 0; i < rowCnt; i++) {
      GenericRow row = new GenericRow();
      row.putValue(STRING_COLUMN, STRING_VALUES[i]);
      row.putValue(LONG_COLUMN, LONG_VALUES[i]);
      rows.add(row);
    }
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return new File(TABLE_DATA_DIR, segName);
  }

  private static String getCRC(File segDir, SegmentVersion segVer)
      throws IOException {
    File parentDir = segDir;
    if (segVer == SegmentVersion.v3) {
      parentDir = new File(segDir, "v3");
    }
    File crcFile = new File(parentDir, V1Constants.SEGMENT_CREATION_META);
    try (DataInputStream ds = new DataInputStream(new FileInputStream(crcFile))) {
      return String.valueOf(ds.readLong());
    }
  }

  private static boolean hasInvertedIndex(File segDir, String colName, SegmentVersion segVer)
      throws IOException {
    File parentDir = segDir;
    if (segVer == SegmentVersion.v3) {
      parentDir = new File(segDir, "v3");
    }
    File idxMapFile = new File(parentDir, V1Constants.INDEX_MAP_FILE_NAME);
    return FileUtils.readFileToString(idxMapFile).contains(colName + ".inverted_index");
  }

  private TableConfig createTableConfigWithTier(String tierName, File dataDir) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(Collections
        .singletonList(new TierConfig(tierName, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "3d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tag_OFFLINE", null,
            Collections.singletonMap("dataDir", dataDir.getAbsolutePath())))).build();
  }
}

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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.fetcher.BaseSegmentFetcher;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.SegmentAllIndexPreprocessThrottler;
import org.apache.pinot.segment.local.utils.SegmentDownloadThrottler;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottler;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.local.utils.SegmentStarTreePreprocessThrottler;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.crypt.PinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class BaseTableDataManagerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "BaseTableDataManagerTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final File TABLE_DATA_DIR = new File(TEMP_DIR, OFFLINE_TABLE_NAME);
  private static final String SEGMENT_NAME = "testSegment";
  private static final String TIER_SEGMENT_DIRECTORY_LOADER = "tierBased";
  private static final String TIER_NAME = "coolTier";
  private static final String STRING_COLUMN = "col1";
  private static final String[] STRING_VALUES = {"A", "D", "E", "B", "C"};
  private static final String LONG_COLUMN = "col2";
  private static final long[] LONG_VALUES = {10000L, 20000L, 50000L, 40000L, 30000L};

  private static final TableConfig DEFAULT_TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final TableConfig TIER_TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTierConfigList(List.of(
          new TierConfig(TIER_NAME, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "3d", null,
              TierFactory.PINOT_SERVER_STORAGE_TYPE, "tag_OFFLINE", null,
              Map.of("dataDir", new File(TEMP_DIR, TIER_NAME).getAbsolutePath())))).build();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).addSingleValueDimension(STRING_COLUMN, DataType.STRING)
          .addMetric(LONG_COLUMN, DataType.LONG).build();
  static final SegmentOperationsThrottler SEGMENT_OPERATIONS_THROTTLER = new SegmentOperationsThrottler(
      new SegmentAllIndexPreprocessThrottler(2, 4, true), new SegmentStarTreePreprocessThrottler(2, 4, true),
      new SegmentDownloadThrottler(2, 4, true));

  @BeforeClass
  public void setUp()
      throws Exception {
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @BeforeMethod
  public void setUpMethod()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    initSegmentFetcher();
  }

  @AfterMethod
  public void tearDownMethod()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  public static void initSegmentFetcher()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(BaseSegmentFetcher.RETRY_COUNT_CONFIG_KEY, 3);
    properties.put(BaseSegmentFetcher.RETRY_WAIT_MS_CONFIG_KEY, 100);
    properties.put(BaseSegmentFetcher.RETRY_DELAY_SCALE_FACTOR_CONFIG_KEY, 5);
    SegmentFetcherFactory.init(new PinotConfiguration(properties));

    // Setup crypter
    properties.put("class.fakePinotCrypter", BaseTableDataManagerTest.FakePinotCrypter.class.getName());
    PinotCrypterFactory.init(new PinotConfiguration(properties));
  }

  @Test
  public void testReloadSegmentNewData()
      throws Exception {
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    SegmentMetadata localMetadata = mock(SegmentMetadata.class);
    when(localMetadata.getCrc()).thenReturn("0");

    BaseTableDataManager tableDataManager = createTableManager();
    File dataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);
    assertFalse(dataDir.exists());
    tableDataManager.reloadSegment(SEGMENT_NAME, new IndexLoadingConfig(), zkMetadata, localMetadata, false);
    assertTrue(dataDir.exists());
    assertEquals(new SegmentMetadataImpl(dataDir).getTotalDocs(), 5);
  }

  @Test
  public void testReloadSegmentNewDataNewTier()
      throws Exception {
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);
    zkMetadata.setTier(TIER_NAME);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    SegmentMetadata localMetadata = mock(SegmentMetadata.class);
    when(localMetadata.getCrc()).thenReturn("0");

    // No dataDir for coolTier, thus stay on default tier.
    BaseTableDataManager tableDataManager = createTableManager();
    File defaultDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);
    assertFalse(defaultDataDir.exists());
    tableDataManager.reloadSegment(SEGMENT_NAME, createTierIndexLoadingConfig(DEFAULT_TABLE_CONFIG), zkMetadata,
        localMetadata, false);
    assertTrue(defaultDataDir.exists());
    assertEquals(new SegmentMetadataImpl(defaultDataDir).getTotalDocs(), 5);

    // Configured dataDir for coolTier, thus move to new dir.
    tableDataManager = createTableManager();
    tableDataManager.reloadSegment(SEGMENT_NAME, createTierIndexLoadingConfig(TIER_TABLE_CONFIG), zkMetadata,
        localMetadata, false);
    File tierDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME, TIER_NAME, TIER_TABLE_CONFIG);
    assertTrue(tierDataDir.exists());
    assertFalse(defaultDataDir.exists());
    assertEquals(new SegmentMetadataImpl(tierDataDir).getTotalDocs(), 5);
  }

  @Test
  public void testReloadSegmentUseLocalCopy()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v1, 5);
    long crc = getCRC(indexDir);

    // Same CRCs so load the local segment directory directly.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getCrc()).thenReturn(crc);
    SegmentMetadata localMetadata = mock(SegmentMetadata.class);
    when(localMetadata.getCrc()).thenReturn(Long.toString(crc));

    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.reloadSegment(SEGMENT_NAME, new IndexLoadingConfig(), zkMetadata, localMetadata, false);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);

    FileUtils.deleteQuietly(indexDir);
    try {
      tableDataManager.reloadSegment(SEGMENT_NAME, new IndexLoadingConfig(), zkMetadata, localMetadata, false);
      fail();
    } catch (Exception e) {
      // As expected, segment reloading fails due to missing the local segment dir.
    }
  }

  @Test
  public void testReloadSegmentUseLocalCopyNewTier()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v1, 5);
    long crc = getCRC(indexDir);

    // Same CRCs so load the local segment directory directly.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getCrc()).thenReturn(crc);
    when(zkMetadata.getTier()).thenReturn(TIER_NAME);
    SegmentMetadata localMetadata = mock(SegmentMetadata.class);
    when(localMetadata.getCrc()).thenReturn(Long.toString(crc));

    // No dataDir for coolTier, thus stay on default tier.
    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.reloadSegment(SEGMENT_NAME, createTierIndexLoadingConfig(DEFAULT_TABLE_CONFIG), zkMetadata,
        localMetadata, false);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);

    // Configured dataDir for coolTier, thus move to new dir.
    tableDataManager = createTableManager();
    tableDataManager.reloadSegment(SEGMENT_NAME, createTierIndexLoadingConfig(TIER_TABLE_CONFIG), zkMetadata,
        localMetadata, false);
    File tierDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME, TIER_NAME, TIER_TABLE_CONFIG);
    assertTrue(tierDataDir.exists());
    assertFalse(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(tierDataDir).getTotalDocs(), 5);
  }

  @Test
  public void testReloadSegmentConvertVersion()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v1, 5);
    long crc = getCRC(indexDir);

    // Same CRCs so load the local segment directory directly.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getCrc()).thenReturn(crc);
    SegmentMetadata localMetadata = mock(SegmentMetadata.class);
    when(localMetadata.getCrc()).thenReturn(Long.toString(crc));

    // Require to use v3 format.
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);

    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.reloadSegment(SEGMENT_NAME, indexLoadingConfig, zkMetadata, localMetadata, false);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);
  }

  @Test
  public void testReloadSegmentAddIndex()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);
    assertFalse(hasInvertedIndex(indexDir, STRING_COLUMN));
    assertFalse(hasInvertedIndex(indexDir, LONG_COLUMN));

    // Same CRCs so load the local segment directory directly.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getCrc()).thenReturn(crc);
    SegmentMetadata localMetadata = mock(SegmentMetadata.class);
    when(localMetadata.getCrc()).thenReturn(Long.toString(crc));

    // Require to add indices.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of(STRING_COLUMN, LONG_COLUMN)).build();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);

    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.reloadSegment(SEGMENT_NAME, indexLoadingConfig, zkMetadata, localMetadata, false);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);
    assertTrue(hasInvertedIndex(indexDir, STRING_COLUMN));
    assertTrue(hasInvertedIndex(indexDir, LONG_COLUMN));
  }

  @Test
  public void testReloadSegmentForceDownload()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    SegmentZKMetadata zkMetadata =
        makeRawSegment(indexDir, new File(TEMP_DIR, SEGMENT_NAME + TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION),
            false);

    // Same CRC but force to download.
    BaseTableDataManager tableDataManager = createTableManager();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(Long.parseLong(segmentMetadata.getCrc()), zkMetadata.getCrc());

    // Remove the local segment dir. Segment reloading fails unless force to download.
    FileUtils.deleteQuietly(indexDir);
    try {
      tableDataManager.reloadSegment(SEGMENT_NAME, new IndexLoadingConfig(), zkMetadata, segmentMetadata, false);
      fail();
    } catch (Exception e) {
      // As expected, segment reloading fails due to missing the local segment dir.
    }

    tableDataManager.reloadSegment(SEGMENT_NAME, new IndexLoadingConfig(), zkMetadata, segmentMetadata, true);
    assertTrue(indexDir.exists());
    segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(Long.parseLong(segmentMetadata.getCrc()), zkMetadata.getCrc());
    assertEquals(segmentMetadata.getTotalDocs(), 5);
  }

  @Test
  public void testReplaceSegmentNewData()
      throws Exception {
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    ImmutableSegmentDataManager segmentDataManager = createImmutableSegmentDataManager(SEGMENT_NAME, 0);

    BaseTableDataManager tableDataManager = createTableManager();
    File dataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);
    assertFalse(dataDir.exists());
    tableDataManager.replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, new IndexLoadingConfig());
    assertTrue(dataDir.exists());
    assertEquals(new SegmentMetadataImpl(dataDir).getTotalDocs(), 5);
  }

  @Test
  public void testReplaceSegmentNewDataNewTier()
      throws Exception {
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);
    zkMetadata.setTier(TIER_NAME);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    ImmutableSegmentDataManager segmentDataManager = createImmutableSegmentDataManager(SEGMENT_NAME, 0);

    // No dataDir for coolTier, thus stay on default tier.
    BaseTableDataManager tableDataManager = createTableManager();
    File defaultDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);
    assertFalse(defaultDataDir.exists());
    tableDataManager.replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata,
        createTierIndexLoadingConfig(DEFAULT_TABLE_CONFIG));
    assertTrue(defaultDataDir.exists());
    assertEquals(new SegmentMetadataImpl(defaultDataDir).getTotalDocs(), 5);

    // Configured dataDir for coolTier, thus move to new dir.
    tableDataManager = createTableManager();
    tableDataManager.replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata,
        createTierIndexLoadingConfig(TIER_TABLE_CONFIG));
    File tierDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME, TIER_NAME, TIER_TABLE_CONFIG);
    assertTrue(tierDataDir.exists());
    assertFalse(defaultDataDir.exists());
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(tierDataDir);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    assertEquals(segmentMetadata.getIndexDir(), tierDataDir);
  }

  @Test
  public void testReplaceSegmentNoop()
      throws Exception {
    String segmentName = "seg01";
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(segmentName);
    when(zkMetadata.getCrc()).thenReturn(1024L);

    ImmutableSegmentDataManager segmentDataManager = createImmutableSegmentDataManager(segmentName, 1024L);

    BaseTableDataManager tableDataManager = createTableManager();
    assertFalse(tableDataManager.getSegmentDataDir(segmentName).exists());
    tableDataManager.replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, new IndexLoadingConfig());
    // As CRC is same, the index dir is left as is, so not get created by the test.
    assertFalse(tableDataManager.getSegmentDataDir(segmentName).exists());
  }

  @Test
  public void testAddNewSegmentUseLocalCopy()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getCrc()).thenReturn(crc);
    when(zkMetadata.getDownloadUrl()).thenReturn("file://somewhere");

    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.addNewOnlineSegment(zkMetadata, new IndexLoadingConfig());
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);

    FileUtils.deleteQuietly(indexDir);
    try {
      tableDataManager.addNewOnlineSegment(zkMetadata, new IndexLoadingConfig());
      fail();
    } catch (Exception e) {
      // As expected, when local segment dir is missing, it tries to download
      // raw segment from deep store, but it would fail with bad download uri.
      assertEquals(e.getMessage(), "Operation failed after 3 attempts");
    }
  }

  @Test
  public void testAddSegmentUseLocalCopyNewTier()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getCrc()).thenReturn(crc);
    when(zkMetadata.getDownloadUrl()).thenReturn("file://somewhere");
    when(zkMetadata.getTier()).thenReturn(TIER_NAME);

    // No dataDir for coolTier, thus stay on default tier.
    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.addNewOnlineSegment(zkMetadata, createTierIndexLoadingConfig(DEFAULT_TABLE_CONFIG));
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);

    // Configured dataDir for coolTier, thus move to new dir.
    File tierDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME, TIER_NAME, TIER_TABLE_CONFIG);
    assertFalse(tierDataDir.exists());
    // Move segDir to new tier to see if addNewOnlineSegment() can load segDir from new tier directly.
    FileUtils.moveDirectory(indexDir, tierDataDir);
    tableDataManager = createTableManager();
    tableDataManager.addNewOnlineSegment(zkMetadata, createTierIndexLoadingConfig(TIER_TABLE_CONFIG));
    assertEquals(new SegmentMetadataImpl(tierDataDir).getTotalDocs(), 5);
  }

  @Test
  public void testAddSegmentUseBackupCopy()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getCrc()).thenReturn(crc);

    BaseTableDataManager tableDataManager = createTableManager();
    File backupDir =
        tableDataManager.getSegmentDataDir(SEGMENT_NAME + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    assertTrue(indexDir.renameTo(backupDir));

    assertFalse(indexDir.exists());
    tableDataManager.addNewOnlineSegment(zkMetadata, new IndexLoadingConfig());
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);
  }

  @Test
  public void testAddSegmentStaleBackupCopy()
      throws Exception {
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);

    // Create a local segment with fewer rows, making its CRC different from the raw segment.
    // So that the raw segment is downloaded and loaded in the end.
    BaseTableDataManager tableDataManager = createTableManager();
    File indexDir = createSegment(SegmentVersion.v3, 3);
    File backupDir =
        tableDataManager.getSegmentDataDir(SEGMENT_NAME + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    assertTrue(indexDir.renameTo(backupDir));

    assertFalse(indexDir.exists());
    tableDataManager.addNewOnlineSegment(zkMetadata, new IndexLoadingConfig());
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);
  }

  @Test
  public void testAddSegmentUpConvertVersion()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v1, 5);
    long crc = getCRC(indexDir);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getCrc()).thenReturn(crc);

    // Require to use v3 format.
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);

    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.addNewOnlineSegment(zkMetadata, indexLoadingConfig);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);
  }

  @Test
  public void testAddSegmentDownConvertVersion()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getCrc()).thenReturn(crc);

    // Require to use v1 format.
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v1);

    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.addNewOnlineSegment(zkMetadata, indexLoadingConfig);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    // The existing segment preprocessing logic doesn't down convert segment format.
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);
  }

  @Test
  public void testAddSegmentAddIndex()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);
    assertFalse(hasInvertedIndex(indexDir, STRING_COLUMN));
    assertFalse(hasInvertedIndex(indexDir, LONG_COLUMN));

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getCrc()).thenReturn(crc);

    // Require to add indices.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of(STRING_COLUMN, LONG_COLUMN)).build();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);

    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.addNewOnlineSegment(zkMetadata, indexLoadingConfig);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    assertTrue(hasInvertedIndex(indexDir, STRING_COLUMN));
    assertTrue(hasInvertedIndex(indexDir, LONG_COLUMN));
  }

  @Test
  public void testDownloadAndDecrypt()
      throws Exception {
    File tempDir = new File(TEMP_DIR, "testDownloadAndDecrypt");
    String fileName = "tmp.txt";
    FileUtils.write(new File(tempDir, fileName), "this is from somewhere remote");
    String tarFileName = SEGMENT_NAME + TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION;
    File tempTarFile = new File(TEMP_DIR, tarFileName);
    TarCompressionUtils.createCompressedTarFile(tempDir, tempTarFile);

    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getDownloadUrl()).thenReturn("file://" + tempTarFile.getAbsolutePath());

    BaseTableDataManager tableDataManager = createTableManager();
    File indexDir = tableDataManager.downloadSegment(zkMetadata);
    assertEquals(FileUtils.readFileToString(new File(indexDir, fileName)), "this is from somewhere remote");

    FileUtils.deleteDirectory(indexDir);
    when(zkMetadata.getCrypterName()).thenReturn("fakePinotCrypter");
    indexDir = tableDataManager.downloadSegment(zkMetadata);
    assertEquals(FileUtils.readFileToString(new File(indexDir, fileName)), "this is from somewhere remote");

    FakePinotCrypter fakeCrypter = (FakePinotCrypter) PinotCrypterFactory.create("fakePinotCrypter");
    assertTrue(fakeCrypter._origFile.getAbsolutePath().endsWith(tarFileName + ".enc"));
    assertTrue(fakeCrypter._decFile.getAbsolutePath().endsWith(tarFileName));

    try {
      // Set maxRetry to 0 to cause retry failure immediately.
      Map<String, Object> properties = new HashMap<>();
      properties.put(BaseSegmentFetcher.RETRY_COUNT_CONFIG_KEY, 0);
      SegmentFetcherFactory.init(new PinotConfiguration(properties));
      tableDataManager.downloadSegment(zkMetadata);
      fail();
    } catch (AttemptsExceededException e) {
      assertEquals(e.getMessage(), "Operation failed after 0 attempts");
    }
  }

  @Test
  public void testUntarAndMoveSegment()
      throws IOException {
    BaseTableDataManager tableDataManager = createTableManager();
    File tempRootDir = tableDataManager.getTmpSegmentDataDir("test-untar-move");

    // All input and intermediate files are put in the tempRootDir.
    File tempTar = new File(tempRootDir, SEGMENT_NAME + TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION);
    File tempInputDir = new File(tempRootDir, "input");
    FileUtils.write(new File(tempInputDir, "tmp.txt"), "this is in segment dir");
    TarCompressionUtils.createCompressedTarFile(tempInputDir, tempTar);
    FileUtils.deleteQuietly(tempInputDir);

    // The destination is the segment directory at the same level of tempRootDir.
    File untarredFile = tableDataManager.untarAndMoveSegment(SEGMENT_NAME, tempTar, tempRootDir);
    assertEquals(untarredFile, tableDataManager.getSegmentDataDir(SEGMENT_NAME));
    assertEquals(FileUtils.readFileToString(new File(untarredFile, "tmp.txt")), "this is in segment dir");

    try {
      tableDataManager.untarAndMoveSegment(SEGMENT_NAME, new File(tempRootDir, "unknown.txt"), TEMP_DIR);
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
      origFile.renameTo(decFile);
    }
  }

  static OfflineTableDataManager createTableManager() {
    return createTableManager(createDefaultInstanceDataManagerConfig());
  }

  private static OfflineTableDataManager createTableManager(InstanceDataManagerConfig instanceDataManagerConfig) {
    OfflineTableDataManager tableDataManager = new OfflineTableDataManager();
    tableDataManager.init(instanceDataManagerConfig, mock(HelixManager.class), new SegmentLocks(), DEFAULT_TABLE_CONFIG,
        SCHEMA, new SegmentReloadSemaphore(1), Executors.newSingleThreadExecutor(), null, null,
        SEGMENT_OPERATIONS_THROTTLER);
    return tableDataManager;
  }

  private static InstanceDataManagerConfig createDefaultInstanceDataManagerConfig() {
    InstanceDataManagerConfig config = mock(InstanceDataManagerConfig.class);
    when(config.getInstanceDataDir()).thenReturn(TEMP_DIR.getAbsolutePath());
    return config;
  }

  private static File createSegment(SegmentVersion segmentVersion, int numRows)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(DEFAULT_TABLE_CONFIG, SCHEMA);
    config.setOutDir(TABLE_DATA_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);
    config.setSegmentVersion(segmentVersion);
    List<GenericRow> rows = new ArrayList<>(3);
    for (int i = 0; i < numRows; i++) {
      GenericRow row = new GenericRow();
      row.putValue(STRING_COLUMN, STRING_VALUES[i]);
      row.putValue(LONG_COLUMN, LONG_VALUES[i]);
      rows.add(row);
    }
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return new File(TABLE_DATA_DIR, SEGMENT_NAME);
  }

  private static SegmentZKMetadata createRawSegment(SegmentVersion segmentVersion, int numRows)
      throws Exception {
    File indexDir = createSegment(segmentVersion, numRows);
    return makeRawSegment(indexDir,
        new File(TEMP_DIR, SEGMENT_NAME + TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION), true);
  }

  private static SegmentZKMetadata makeRawSegment(File indexDir, File rawSegmentFile, boolean deleteIndexDir)
      throws Exception {
    long crc = getCRC(indexDir);
    SegmentZKMetadata zkMetadata = new SegmentZKMetadata(SEGMENT_NAME);
    TarCompressionUtils.createCompressedTarFile(indexDir, rawSegmentFile);
    zkMetadata.setDownloadUrl("file://" + rawSegmentFile.getAbsolutePath());
    zkMetadata.setCrc(crc);
    if (deleteIndexDir) {
      FileUtils.deleteQuietly(indexDir);
    }
    return zkMetadata;
  }

  private static long getCRC(File indexDir)
      throws IOException {
    File creationMetaFile = SegmentDirectoryPaths.findCreationMetaFile(indexDir);
    assertNotNull(creationMetaFile);
    try (DataInputStream in = new DataInputStream(new FileInputStream(creationMetaFile))) {
      return in.readLong();
    }
  }

  private IndexLoadingConfig createTierIndexLoadingConfig(TableConfig tableConfig) {
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getSegmentDirectoryLoader()).thenReturn(TIER_SEGMENT_DIRECTORY_LOADER);
    when(instanceDataManagerConfig.getConfig()).thenReturn(new PinotConfiguration());
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(instanceDataManagerConfig, tableConfig, null);
    indexLoadingConfig.setTableDataDir(TEMP_DIR.getAbsolutePath() + File.separator + tableConfig.getTableName());
    indexLoadingConfig.setSegmentTier(TIER_NAME);
    return indexLoadingConfig;
  }

  private ImmutableSegmentDataManager createImmutableSegmentDataManager(String segmentName, long crc) {
    ImmutableSegmentDataManager segmentDataManager = mock(ImmutableSegmentDataManager.class);
    when(segmentDataManager.getSegmentName()).thenReturn(segmentName);
    ImmutableSegment immutableSegment = mock(ImmutableSegment.class);
    when(segmentDataManager.getSegment()).thenReturn(immutableSegment);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(immutableSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentMetadata.getCrc()).thenReturn(Long.toString(crc));
    return segmentDataManager;
  }

  private static boolean hasInvertedIndex(File indexDir, String columnName)
      throws IOException {
    File indexMapFile =
        new File(new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME), V1Constants.INDEX_MAP_FILE_NAME);
    return FileUtils.readFileToString(indexMapFile, StandardCharsets.UTF_8).contains(columnName + ".inverted_index");
  }
}

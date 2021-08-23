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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.crypt.PinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
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


public class BaseTableDataManagerAddSegmentTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "OfflineTableDataManagerTest");

  private static final String TABLE_NAME = "__table01__";

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

  private BaseTableDataManager makeTestableManager() {
    TableDataManagerConfig config = mock(TableDataManagerConfig.class);
    when(config.getTableName()).thenReturn(TABLE_NAME);
    when(config.getDataDir()).thenReturn(new File(TEMP_DIR, TABLE_NAME).getAbsolutePath());

    OfflineTableDataManager tableDataManager = new OfflineTableDataManager();
    tableDataManager.init(config, "dummyInstance", mock(ZkHelixPropertyStore.class),
        new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), mock(HelixManager.class), null);
    tableDataManager.start();
    return tableDataManager;
  }

  @Test
  public void testAddOrReplaceSegmentNewData()
      throws Exception {
    BaseTableDataManager tmgr = makeTestableManager();
    File tempRootDir = tmgr.getSegmentDataDir("test-new-data");

    // Create an empty segment and compress it to tar.gz as the one in deep store.
    // All input and intermediate files are put in the tempRootDir.
    File tempTar = new File(tempRootDir, "seg01" + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    File tempInputDir = new File(tempRootDir, "seg01_input");
    FileUtils.write(new File(tempInputDir, "metadata.properties"), "segment.total.docs=0\nsegment.name=seg01");
    TarGzCompressionUtils.createTarGzFile(tempInputDir, tempTar);
    FileUtils.deleteQuietly(tempInputDir);

    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getDownloadUrl()).thenReturn("file://" + tempTar.getAbsolutePath());
    when(zkmd.getCrc()).thenReturn(Long.valueOf(1024));

    // Different CRCs leading to segment download.
    SegmentMetadata llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn("10240");

    assertFalse(tmgr.getSegmentDataDir("seg01").exists());
    tmgr.addOrReplaceSegment("seg01", newDummyIndexLoadingConfig(), llmd, zkmd, false);
    assertTrue(tmgr.getSegmentDataDir("seg01").exists());
  }

  @Test
  public void testAddOrReplaceSegmentForceDownload()
      throws Exception {
    BaseTableDataManager tmgr = makeTestableManager();
    File tempRootDir = tmgr.getSegmentDataDir("test-force-download");

    // Create an empty segment and compress it to tar.gz as the one in deep store.
    // All input and intermediate files are put in the tempRootDir.
    File tempTar = new File(tempRootDir, "seg01" + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    File tempInputDir = new File(tempRootDir, "seg01_input");
    FileUtils.write(new File(tempInputDir, "metadata.properties"), "segment.total.docs=0\nsegment.name=seg01");
    TarGzCompressionUtils.createTarGzFile(tempInputDir, tempTar);
    FileUtils.deleteQuietly(tempInputDir);

    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getDownloadUrl()).thenReturn("file://" + tempTar.getAbsolutePath());
    when(zkmd.getCrc()).thenReturn(Long.valueOf(1024));

    // Same CRC but force to download
    SegmentMetadata llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn("1024");

    assertFalse(tmgr.getSegmentDataDir("seg01").exists());
    tmgr.addOrReplaceSegment("seg01", newDummyIndexLoadingConfig(), llmd, zkmd, true);
    assertTrue(tmgr.getSegmentDataDir("seg01").exists());
  }

  @Test
  public void testAddOrReplaceSegmentNoop()
      throws Exception {
    BaseTableDataManager tmgr = makeTestableManager();

    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getCrc()).thenReturn(Long.valueOf(1024));

    SegmentMetadata llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn("1024");

    assertFalse(tmgr.getSegmentDataDir("seg01").exists());
    tmgr.addOrReplaceSegment("seg01", newDummyIndexLoadingConfig(), llmd, zkmd, false);
    // As CRC is same, the index dir is left as is, so not get created by the test.
    assertFalse(tmgr.getSegmentDataDir("seg01").exists());
  }

  @Test
  public void testAddOrReplaceSegmentRecovered()
      throws Exception {
    BaseTableDataManager tmgr = makeTestableManager();

    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    // Make this equal to the default crc value, so no need to make a dummy creation.meta file.
    when(zkmd.getCrc()).thenReturn(Long.MIN_VALUE);

    File backup = tmgr.getSegmentDataDir("seg01" + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    FileUtils.write(new File(backup, "metadata.properties"), "segment.total.docs=0\nsegment.name=seg01");

    assertFalse(tmgr.getSegmentDataDir("seg01").exists());
    tmgr.addOrReplaceSegment("seg01", newDummyIndexLoadingConfig(), null, zkmd, false);
    assertTrue(tmgr.getSegmentDataDir("seg01").exists());
  }

  @Test
  public void testAddOrReplaceSegmentNotRecovered()
      throws Exception {
    BaseTableDataManager tmgr = makeTestableManager();
    File tempRootDir = tmgr.getSegmentDataDir("test-force-download");

    // Create an empty segment and compress it to tar.gz as the one in deep store.
    // All input and intermediate files are put in the tempRootDir.
    File tempTar = new File(tempRootDir, "seg01" + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    File tempInputDir = new File(tempRootDir, "seg01_input");
    FileUtils.write(new File(tempInputDir, "metadata.properties"), "segment.total.docs=0\nsegment.name=seg01");
    TarGzCompressionUtils.createTarGzFile(tempInputDir, tempTar);
    FileUtils.deleteQuietly(tempInputDir);

    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getDownloadUrl()).thenReturn("file://" + tempTar.getAbsolutePath());
    when(zkmd.getCrc()).thenReturn(Long.valueOf(1024));

    // Though can recover from backup, but CRC is different.
    File backup = tmgr.getSegmentDataDir("seg01" + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    FileUtils.write(new File(backup, "metadata.properties"), "segment.total.docs=0\nsegment.name=seg01");

    assertFalse(tmgr.getSegmentDataDir("seg01").exists());
    tmgr.addOrReplaceSegment("seg01", newDummyIndexLoadingConfig(), null, zkmd, true);
    assertTrue(tmgr.getSegmentDataDir("seg01").exists());
  }

  @Test
  public void testDownloadAndDecrypt()
      throws Exception {
    File tempInput = new File(TEMP_DIR, "tmp.txt");
    FileUtils.write(tempInput, "this is from somewhere remote");

    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getDownloadUrl()).thenReturn("file://" + tempInput.getAbsolutePath());

    BaseTableDataManager tmgr = makeTestableManager();
    File tempRootDir = tmgr.getSegmentDataDir("test-download-decrypt");

    File tarFile = tmgr.downloadAndDecrypt("seg01", zkmd, tempRootDir);
    assertEquals(FileUtils.readFileToString(tarFile), "this is from somewhere remote");

    when(zkmd.getCrypterName()).thenReturn("fakePinotCrypter");
    tarFile = tmgr.downloadAndDecrypt("seg01", zkmd, tempRootDir);
    assertEquals(FileUtils.readFileToString(tarFile), "this is from somewhere remote");

    FakePinotCrypter fakeCrypter = (FakePinotCrypter) PinotCrypterFactory.create("fakePinotCrypter");
    assertTrue(fakeCrypter._origFile.getAbsolutePath().endsWith("__table01__/test-download-decrypt/seg01.tar.gz.enc"));
    assertTrue(fakeCrypter._decFile.getAbsolutePath().endsWith("__table01__/test-download-decrypt/seg01.tar.gz"));

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
    BaseTableDataManager tmgr = makeTestableManager();
    File tempRootDir = tmgr.getSegmentDataDir("test-untar-move");

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

  @Test
  public void testIsNewSegmentMetadata()
      throws IOException {
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getCrc()).thenReturn(Long.valueOf(1024));
    assertTrue(BaseTableDataManager.isNewSegment(zkmd, null));

    SegmentMetadata llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn("1024");
    assertFalse(BaseTableDataManager.isNewSegment(zkmd, llmd));

    llmd = mock(SegmentMetadata.class);
    when(llmd.getCrc()).thenReturn("10245");
    assertTrue(BaseTableDataManager.isNewSegment(zkmd, llmd));
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

  private static IndexLoadingConfig newDummyIndexLoadingConfig() {
    IndexLoadingConfig indexLoadingConfig = mock(IndexLoadingConfig.class);
    when(indexLoadingConfig.getReadMode()).thenReturn(ReadMode.mmap);
    when(indexLoadingConfig.getSegmentVersion()).thenReturn(SegmentVersion.v3);
    return indexLoadingConfig;
  }
}

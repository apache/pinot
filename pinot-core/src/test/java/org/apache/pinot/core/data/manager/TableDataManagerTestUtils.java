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
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.fetcher.BaseSegmentFetcher;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TableDataManagerTestUtils {
  private TableDataManagerTestUtils() {
  }

  public static long getCRC(File segDir, SegmentVersion segVer)
      throws IOException {
    File parentDir = segDir;
    if (segVer == SegmentVersion.v3) {
      parentDir = new File(segDir, "v3");
    }
    File crcFile = new File(parentDir, V1Constants.SEGMENT_CREATION_META);
    try (DataInputStream ds = new DataInputStream(new FileInputStream(crcFile))) {
      return ds.readLong();
    }
  }

  public static SegmentZKMetadata makeRawSegment(String segName, File localSegDir, File rawSegDir,
      boolean deleteLocalSegDir)
      throws Exception {
    long segCrc = TableDataManagerTestUtils.getCRC(localSegDir, SegmentVersion.v3);
    SegmentZKMetadata zkmd = new SegmentZKMetadata(segName);
    TarGzCompressionUtils.createTarGzFile(localSegDir, rawSegDir);
    zkmd.setDownloadUrl("file://" + rawSegDir.getAbsolutePath());
    zkmd.setCrc(segCrc);
    if (deleteLocalSegDir) {
      FileUtils.deleteQuietly(localSegDir);
    }
    return zkmd;
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

  public static IndexLoadingConfig createIndexLoadingConfig(String segDirLoader, TableConfig tableConfig,
      @Nullable Schema schema) {
    InstanceDataManagerConfig idmc = mock(InstanceDataManagerConfig.class);
    when(idmc.getSegmentDirectoryLoader()).thenReturn(segDirLoader);
    when(idmc.getConfig()).thenReturn(new PinotConfiguration());
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(idmc, tableConfig, schema);
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    return indexLoadingConfig;
  }

  public static IndexLoadingConfig createIndexLoadingConfig() {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    return indexLoadingConfig;
  }
}

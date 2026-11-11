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
package org.apache.pinot.server.predownload;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.server.predownload.PredownloadTestUtil.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class PredownloadTableInfoTest {
  private PredownloadTableInfo _predownloadTableInfo;
  private TableConfig _tableConfig;
  private InstanceDataManagerConfig _instanceDataManagerConfig;
  private PinotConfiguration _pinotConfiguration;

  @BeforeClass
  public void setUp() {
    _pinotConfiguration = getPinotConfiguration();
    _tableConfig = mock(TableConfig.class);
    when(_tableConfig.getIndexingConfig()).thenReturn(new IndexingConfig());
    Schema schema = new Schema();
    _instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(_instanceDataManagerConfig.getConfig()).thenReturn(_pinotConfiguration);
    _predownloadTableInfo = new PredownloadTableInfo(TABLE_NAME, _tableConfig, schema, _instanceDataManagerConfig);
  }

  @Test
  public void testGetter() {
    assertEquals(_predownloadTableInfo.getTableConfig(), _tableConfig);
  }

  @Test
  public void testLoadSegmentFromLocal()
      throws Exception {
    File tempDir = Files.createTempDirectory("predownload-table-test-").toFile();
    try {
      PredownloadSegmentInfo predownloadSegmentInfo = new PredownloadSegmentInfo(TABLE_NAME, SEGMENT_NAME);
      SegmentZKMetadata metadata = createSegmentZKMetadata();
      predownloadSegmentInfo.updateSegmentInfo(metadata);

      when(_tableConfig.getTableName()).thenReturn(TABLE_NAME);
      when(_instanceDataManagerConfig.getInstanceDataDir()).thenReturn(tempDir.getAbsolutePath());
      when(_instanceDataManagerConfig.getTierConfigs()).thenReturn(null);

      // Segment directory does not exist — returns false
      assertFalse(_predownloadTableInfo.loadSegmentFromLocal(predownloadSegmentInfo));
      assertFalse(predownloadSegmentInfo.isDownloaded());

      // Segment directory exists but no creation.meta — returns false
      File segDir = new File(tempDir, TABLE_NAME + "/" + SEGMENT_NAME);
      segDir.mkdirs();
      assertFalse(_predownloadTableInfo.loadSegmentFromLocal(predownloadSegmentInfo));
      assertFalse(predownloadSegmentInfo.isDownloaded());

      // creation.meta present with matching CRC — returns true and populates size
      File creationMeta = new File(segDir, "creation.meta");
      try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(creationMeta))) {
        dos.writeLong(CRC);
        dos.writeLong(System.currentTimeMillis());
      }
      org.apache.commons.io.FileUtils.writeByteArrayToFile(new File(segDir, "columns.psf"), new byte[]{1, 2, 3});
      assertTrue(_predownloadTableInfo.loadSegmentFromLocal(predownloadSegmentInfo));
      assertEquals(predownloadSegmentInfo.getLocalCrc(), String.valueOf(CRC));
      assertTrue(predownloadSegmentInfo.isDownloaded());
      assertTrue(predownloadSegmentInfo.getLocalSizeBytes() > 0);

      // creation.meta present with different CRC — returns false
      try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(creationMeta))) {
        dos.writeLong(CRC + 1);
        dos.writeLong(System.currentTimeMillis());
      }
      assertFalse(_predownloadTableInfo.loadSegmentFromLocal(predownloadSegmentInfo));
      assertEquals(predownloadSegmentInfo.getLocalCrc(), String.valueOf(CRC + 1));
      assertFalse(predownloadSegmentInfo.isDownloaded());
    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(tempDir);
    }
  }
}

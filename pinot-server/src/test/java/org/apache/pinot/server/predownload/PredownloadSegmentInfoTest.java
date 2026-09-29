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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.server.predownload.PredownloadTestUtil.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class PredownloadSegmentInfoTest {

  private PredownloadSegmentInfo _predownloadSegmentInfo;

  @BeforeClass
  public void setUp() {
    _predownloadSegmentInfo = new PredownloadSegmentInfo(TABLE_NAME, SEGMENT_NAME);
  }

  @Test
  public void testSegmentInfo() {
    assertEquals(_predownloadSegmentInfo.getSegmentName(), SEGMENT_NAME);
    assertEquals(_predownloadSegmentInfo.getTableNameWithType(), TABLE_NAME);
  }

  @Test
  public void testUpdateSegmentInfo() {
    SegmentZKMetadata metadata = createSegmentZKMetadata();
    _predownloadSegmentInfo.updateSegmentInfo(metadata);
    assertEquals(_predownloadSegmentInfo.getCrc(), CRC);
    assertEquals(_predownloadSegmentInfo.getCrypterName(), CRYPTER_NAME);
    assertEquals(_predownloadSegmentInfo.getDownloadUrl(), DOWNLOAD_URL);

    metadata.setDownloadUrl("");
    _predownloadSegmentInfo.updateSegmentInfo(metadata);
    assertFalse(_predownloadSegmentInfo.canBeDownloaded());
  }

  @Test
  public void testInitSegmentDirectory() {
    assertNull(_predownloadSegmentInfo.initSegmentDirectory(null, null));
  }

  @Test
  public void testGetSegmentDataDir() {
    assertThrows(PredownloadException.class, () -> _predownloadSegmentInfo.getSegmentDataDir(null, false));
    SegmentZKMetadata metadata = createSegmentZKMetadata();
    _predownloadSegmentInfo.updateSegmentInfo(metadata);
    assertThrows(PredownloadException.class, () -> _predownloadSegmentInfo.getSegmentDataDir(null, true));
  }

  @Test
  public void testUpdateSegmentInfoFromLocalFile()
      throws Exception {
    PredownloadSegmentInfo segmentInfo = new PredownloadSegmentInfo(TABLE_NAME, SEGMENT_NAME);
    segmentInfo.updateSegmentInfo(createSegmentZKMetadata());

    File tempDir = Files.createTempDirectory("predownload-seg-test-").toFile();
    try {
      // Non-existent path — logs warning and returns without updating fields
      File missingSegDir = new File(tempDir, "missing");
      segmentInfo.updateSegmentInfoFromLocal(missingSegDir);
      assertNull(segmentInfo.getLocalCrc());
      assertEquals(segmentInfo.getLocalSizeBytes(), 0);

      // Regular file (not a directory) — logs warning and returns without updating fields
      File regularFile = new File(tempDir, "not-a-dir");
      assertTrue(regularFile.createNewFile());
      segmentInfo.updateSegmentInfoFromLocal(regularFile);
      assertNull(segmentInfo.getLocalCrc());
      assertEquals(segmentInfo.getLocalSizeBytes(), 0);

      // Segment directory exists but has no creation.meta — fields stay at defaults
      File segDir = new File(tempDir, SEGMENT_NAME);
      assertTrue(segDir.mkdirs());
      segmentInfo.updateSegmentInfoFromLocal(segDir);
      assertNull(segmentInfo.getLocalCrc());
      assertEquals(segmentInfo.getLocalSizeBytes(), 0);

      // creation.meta present with matching CRC — fields populated, isDownloaded true
      File creationMeta = new File(segDir, "creation.meta");
      try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(creationMeta))) {
        dos.writeLong(CRC);
        dos.writeLong(System.currentTimeMillis());
      }
      org.apache.commons.io.FileUtils.writeByteArrayToFile(new File(segDir, "columns.psf"), new byte[]{1, 2, 3});
      segmentInfo.updateSegmentInfoFromLocal(segDir);
      assertEquals(segmentInfo.getLocalCrc(), String.valueOf(CRC));
      assertTrue(segmentInfo.getLocalSizeBytes() > 0);
      assertTrue(segmentInfo.isDownloaded());

      // creation.meta present with different CRC — localCrc set, isDownloaded false
      try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(creationMeta))) {
        dos.writeLong(CRC + 1);
        dos.writeLong(System.currentTimeMillis());
      }
      segmentInfo.updateSegmentInfoFromLocal(segDir);
      assertEquals(segmentInfo.getLocalCrc(), String.valueOf(CRC + 1));
      assertFalse(segmentInfo.isDownloaded());
    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(tempDir);
    }
  }
}

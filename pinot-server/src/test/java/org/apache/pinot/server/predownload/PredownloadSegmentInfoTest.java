/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.server.predownload;

import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.server.predownload.PredownloadTestUtil.*;
import static org.testng.Assert.assertThrows;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;


public class PredownloadSegmentInfoTest {

  private PredownloadSegmentInfo _predownloadSegmentInfo;

  @BeforeClass
  public void setUp() {
    _predownloadSegmentInfo = new PredownloadSegmentInfo(TABLE_NAME, SEGMENT_NAME);
  }

  @Test
  public void testSegmentInfo() {
    assertEquals(SEGMENT_NAME, _predownloadSegmentInfo.getSegmentName());
    assertEquals(TABLE_NAME, _predownloadSegmentInfo.getTableNameWithType());
  }

  @Test
  public void testUpdateSegmentInfo() {
    SegmentZKMetadata metadata = createSegmentZKMetadata();
    _predownloadSegmentInfo.updateSegmentInfo(metadata);
    assertEquals(CRC, _predownloadSegmentInfo.getCrc());
    assertEquals(CRYPTER_NAME, _predownloadSegmentInfo.getCrypterName());
    assertEquals(DOWNLOAD_URL, _predownloadSegmentInfo.getDownloadUrl());

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
}

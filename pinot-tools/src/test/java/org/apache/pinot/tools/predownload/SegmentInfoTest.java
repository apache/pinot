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
package org.apache.pinot.tools.predownload;

import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.tools.predownload.TestUtil.*;
import static org.testng.Assert.assertThrows;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;


public class SegmentInfoTest {

  private SegmentInfo _segmentInfo;

  @BeforeClass
  public void setUp() {
    _segmentInfo = new SegmentInfo(TABLE_NAME, SEGMENT_NAME);
  }

  @Test
  public void testSegmentInfo() {
    assertEquals(SEGMENT_NAME, _segmentInfo.getSegmentName());
    assertEquals(TABLE_NAME, _segmentInfo.getTableNameWithType());
  }

  @Test
  public void testUpdateSegmentInfo() {
    SegmentZKMetadata metadata = createSegmentZKMetadata();
    _segmentInfo.updateSegmentInfo(metadata);
    assertEquals(CRC, _segmentInfo.getCrc());
    assertEquals(CRYPTER_NAME, _segmentInfo.getCrypterName());
    assertEquals(DOWNLOAD_URL, _segmentInfo.getDownloadUrl());

    metadata.setDownloadUrl("");
    _segmentInfo.updateSegmentInfo(metadata);
    assertFalse(_segmentInfo.canBeDownloaded());
  }

  @Test
  public void testInitSegmentDirectory() {
    assertNull(_segmentInfo.initSegmentDirectory(null, null));
  }

  @Test
  public void testGetSegmentDataDir() {
    assertThrows(PredownloadException.class, () -> _segmentInfo.getSegmentDataDir(null, false));
    SegmentZKMetadata metadata = createSegmentZKMetadata();
    _segmentInfo.updateSegmentInfo(metadata);
    assertThrows(PredownloadException.class, () -> _segmentInfo.getSegmentDataDir(null, true));
  }
}

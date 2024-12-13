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

  private SegmentInfo segmentInfo;

  @BeforeClass
  public void setUp() {
    segmentInfo = new SegmentInfo(TABLE_NAME, SEGMENT_NAME);
  }

  @Test
  public void testSegmentInfo() {
    assertEquals(SEGMENT_NAME, segmentInfo.getSegmentName());
    assertEquals(TABLE_NAME, segmentInfo.getTableNameWithType());
  }

  @Test
  public void testUpdateSegmentInfo() {
    SegmentZKMetadata metadata = createSegmentZKMetadata();
    segmentInfo.updateSegmentInfo(metadata);
    assertEquals(CRC, segmentInfo.getCrc());
    assertEquals(CRYPTER_NAME, segmentInfo.getCrypterName());
    assertEquals(DOWNLOAD_URL, segmentInfo.getDownloadUrl());

    metadata.setDownloadUrl("");
    segmentInfo.updateSegmentInfo(metadata);
    assertFalse(segmentInfo.canBeDownloaded());
  }

  @Test
  public void testInitSegmentDirectory() {
    assertNull(segmentInfo.initSegmentDirectory(null, null));
  }

  @Test
  public void testGetSegmentDataDir() {
    assertThrows(PredownloadException.class, () -> segmentInfo.getSegmentDataDir(null, false));
    SegmentZKMetadata metadata = createSegmentZKMetadata();
    segmentInfo.updateSegmentInfo(metadata);
    assertThrows(PredownloadException.class, () -> segmentInfo.getSegmentDataDir(null, true));
  }
}

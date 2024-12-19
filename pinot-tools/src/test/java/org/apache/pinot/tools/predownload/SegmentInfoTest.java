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

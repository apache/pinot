package com.linkedin.pinot.common.segment;

import java.io.File;


public interface SegmentMetadataLoader {

  public SegmentMetadata loadIndexSegmentMetadataFromDir(String segmentDir, ReadMode readMode);

  public SegmentMetadata load(File segmentDir, ReadMode heap) throws Exception;

}

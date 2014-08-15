package com.linkedin.pinot.common.segment;

import java.io.File;


/**
 * SegmentMetadataLoader will load SegmentMetadata from segment directory.
 * 
 * @author xiafu
 *
 */
public interface SegmentMetadataLoader {

  public SegmentMetadata loadIndexSegmentMetadataFromDir(String segmentDir) throws Exception;

  public SegmentMetadata load(File segmentDir) throws Exception;

}

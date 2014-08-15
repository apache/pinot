package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.File;

import org.apache.log4j.Logger;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;


/**
 * An implementation of SegmentMetadataLoader.
 * 
 * @author xiafu
 *
 */
public class ColumnarSegmentMetadataLoader implements SegmentMetadataLoader {

  public static final Logger LOGGER = Logger.getLogger(ColumnarSegmentMetadataLoader.class);

  @Override
  public SegmentMetadata loadIndexSegmentMetadataFromDir(String segmentDir) throws Exception {
    return load(new File(segmentDir));
  }

  @Override
  public SegmentMetadata load(File segmentDir) throws Exception {
    SegmentMetadata segmentMetadata =
        new ColumnarSegmentMetadata(new File(segmentDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    LOGGER.info("Loaded segment metadata for segment : " + segmentMetadata.getName());
    return segmentMetadata;
  }
}

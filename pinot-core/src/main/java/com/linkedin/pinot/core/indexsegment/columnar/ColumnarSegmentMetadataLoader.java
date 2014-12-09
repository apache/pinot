package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.File;

import org.apache.log4j.Logger;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


/**
 *
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 17, 2014
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
    final SegmentMetadata segmentMetadata = new SegmentMetadataImpl(segmentDir);
    LOGGER.info("Loaded segment metadata for segment : " + segmentMetadata.getName());
    return segmentMetadata;
  }
}

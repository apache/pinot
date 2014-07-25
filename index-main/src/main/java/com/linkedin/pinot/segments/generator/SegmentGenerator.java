package com.linkedin.pinot.segments.generator;

import com.linkedin.pinot.raw.record.readers.RecordReader;
import com.linkedin.pinot.raw.record.readers.RecordReaderFactory;
import com.linkedin.pinot.segments.creator.SegmentCreator;
import com.linkedin.pinot.segments.creator.SegmentCreatorFactory;


/**
 * This is the enter point for segment creation.
 * Initialize with setup properties as segmentCreationSpec then build Segment.
 * 
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class SegmentGenerator {

  public static void build(SegmentGeneratorConfiguration segmentCreationSpec) throws Exception {
    RecordReader _dataPublisher = RecordReaderFactory.get(segmentCreationSpec);
    SegmentCreator indexSegmentCreator = SegmentCreatorFactory.get(segmentCreationSpec.getSegmentVersion(), _dataPublisher);
    
    indexSegmentCreator.init(segmentCreationSpec);
    indexSegmentCreator.buildSegment();
    
  }

}

package com.linkedin.pinot.core.indexsegment.generator;

import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreator;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreatorFactory;


/**
 * This is the enter point for segment creation.
 * Initialize with setup properties as segmentCreationSpec then build Segment.
 * 
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class SegmentGenerator {

  public static void build(ChunkGeneratorConfiguration segmentCreationSpec) throws Exception {
    RecordReader _dataPublisher = RecordReaderFactory.get(segmentCreationSpec);
    SegmentCreator indexSegmentCreator =
        SegmentCreatorFactory.get(segmentCreationSpec.getSegmentVersion(), _dataPublisher);

    indexSegmentCreator.init(segmentCreationSpec);
    indexSegmentCreator.buildSegment();

  }

}

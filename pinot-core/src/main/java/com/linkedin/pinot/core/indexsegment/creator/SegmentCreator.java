package com.linkedin.pinot.core.indexsegment.creator;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.ChunkGeneratorConfiguration;


/**
 * Initialized with fieldSpec.
 * Call addRow(...) to index row events.
 * After finished adding, call buildSegment() to create a segment. 
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public interface SegmentCreator {
  public void init(ChunkGeneratorConfiguration segmentCreationSpec);
  public IndexSegment buildSegment() throws Exception;

}

package com.linkedin.pinot.segments.creator;

import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.segments.generator.SegmentGeneratorConfiguration;


/**
 * Initialized with fieldSpec.
 * Call addRow(...) to index row events.
 * After finished adding, call buildSegment() to create a segment. 
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public interface SegmentCreator {
  public void init(SegmentGeneratorConfiguration segmentCreationSpec);

  public IndexSegment buildSegment() throws Exception;

}

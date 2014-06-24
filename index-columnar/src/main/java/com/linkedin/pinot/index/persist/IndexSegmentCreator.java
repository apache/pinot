package com.linkedin.pinot.index.persist;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.index.data.GenericRow;
import com.linkedin.pinot.index.segment.IndexSegment;


/**
 * Initialized with fieldSpec.
 * Call addRow(...) to index row events.
 * After finished adding, call buildSegment() to create a segment. 
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public interface IndexSegmentCreator {
  public void init(Configuration segmentCreationSpec);

  public void addRow(GenericRow genericRow);

  public IndexSegment buildSegment();

  /**
   * If want to reuse this creator, put something here
   */
  public void rewind(Configuration segmentCreationSpec);
}

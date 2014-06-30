package com.linkedin.pinot.server.partition;

import com.linkedin.pinot.index.segment.IndexSegment;


/**
 * An immutable wrapper of IndexSegment.
 *  
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class SegmentDataManager {

  private final IndexSegment _indexSegment;

  public SegmentDataManager(IndexSegment indexSegment) {
    _indexSegment = indexSegment;
  }

  public IndexSegment getSegment() {
    return _indexSegment;
  }
}

package com.linkedin.pinot.query.pruner;

import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.query.request.Query;


public interface SegmentPrunerService {
  /**
   * @param segment
   * @param request
   * @return
   */
  public boolean prune(IndexSegment segment, Query query);
}

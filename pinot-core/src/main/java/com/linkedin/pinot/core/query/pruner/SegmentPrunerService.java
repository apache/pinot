package com.linkedin.pinot.core.query.pruner;

import com.linkedin.pinot.common.query.request.Query;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public interface SegmentPrunerService {
  /**
   * @param segment
   * @param request
   * @return
   */
  public boolean prune(IndexSegment segment, Query query);
}

package com.linkedin.pinot.core.query.pruner;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public interface SegmentPrunerService {
  /**
   * @param segment
   * @param request
   * @return
   */
  public boolean prune(IndexSegment segment, BrokerRequest query);
}

package com.linkedin.pinot.core.query.pruner;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.common.query.request.Query;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public interface SegmentPruner {

  /**
   * 
   * @param config
   */
  public void init(Configuration config);

  /**
   * Returns true if the given segment can be pruned
   *
   * @param segment
   * @param request
   * @return true if the given segment is pruned.
   */
  public boolean prune(IndexSegment segment, Query query);
}

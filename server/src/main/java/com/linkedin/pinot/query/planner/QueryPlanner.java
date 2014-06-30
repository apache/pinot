package com.linkedin.pinot.query.planner;

import java.util.List;

import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.server.partition.SegmentDataManager;


/**
 * QueryPlanner interface will provide different strategy to plan on how to process segments.
 *
 */
public interface QueryPlanner {
  public QueryPlan computeQueryPlan(Query query, List<SegmentDataManager> segmentDataManagerList);
}

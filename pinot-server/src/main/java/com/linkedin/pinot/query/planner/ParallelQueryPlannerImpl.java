package com.linkedin.pinot.query.planner;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.server.partition.SegmentDataManager;


/**
 * An implementation of QueryPlanner, will mark all the segments with 0 degree in the graph. 
 *
 */
public class ParallelQueryPlannerImpl implements QueryPlanner {

  public ParallelQueryPlannerImpl() {
    super();
  }

  @Override
  public QueryPlan computeQueryPlan(Query query, List<SegmentDataManager> segmentDataManagerList) {
    QueryPlanCreator queryPlanCreator = new QueryPlanCreator(query);
    for (SegmentDataManager segmentDataManager : segmentDataManagerList) {
      List<IndexSegment> indexSegmentList = new ArrayList<IndexSegment>();
      indexSegmentList.add(segmentDataManager.getSegment());
      queryPlanCreator.addJobVertexWithDependency(null, new JobVertex(indexSegmentList));
    }
    return queryPlanCreator.buildQueryPlan();
  }
}

package com.linkedin.pinot.query.planner;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.query.planner.JobVertex;
import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.server.partition.SegmentDataManager;


public class SequentialQueryPlannerImpl implements QueryPlanner {

  public SequentialQueryPlannerImpl() {
    super();
  }

  @Override
  public QueryPlan computeQueryPlan(Query query, List<SegmentDataManager> segmentDataManagerList) {
    QueryPlanCreator queryPlanCreator = new QueryPlanCreator(query);
    List<IndexSegment> indexSegmentList = new ArrayList<IndexSegment>();
    for (SegmentDataManager segmentDataManager : segmentDataManagerList) {
      indexSegmentList.add(segmentDataManager.getSegment());
    }
    queryPlanCreator.addJobVertexWithDependency(null, new JobVertex(indexSegmentList));
    return queryPlanCreator.buildQueryPlan();
  }
}

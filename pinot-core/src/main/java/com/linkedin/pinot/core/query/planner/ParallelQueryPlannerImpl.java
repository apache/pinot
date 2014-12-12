package com.linkedin.pinot.core.query.planner;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * An implementation of QueryPlanner, will mark all the segments with 0 degree in the graph. 
 *
 */
public class ParallelQueryPlannerImpl implements QueryPlanner {

  public ParallelQueryPlannerImpl() {
    super();
  }

  @Override
  public QueryPlan computeQueryPlan(BrokerRequest query, List<IndexSegment> indexSegmentList) {
    QueryPlanCreator queryPlanCreator = new QueryPlanCreator(query);
    for (IndexSegment indexSegment : indexSegmentList) {
      List<IndexSegment> vertexSegmentList = new ArrayList<IndexSegment>();
      vertexSegmentList.add(indexSegment);
      queryPlanCreator.addJobVertexWithDependency(null, new JobVertex(vertexSegmentList));
    }
    return queryPlanCreator.buildQueryPlan();
  }

}

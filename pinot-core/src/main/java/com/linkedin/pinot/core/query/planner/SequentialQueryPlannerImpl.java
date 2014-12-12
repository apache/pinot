package com.linkedin.pinot.core.query.planner;

import java.util.List;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public class SequentialQueryPlannerImpl implements QueryPlanner {

  public SequentialQueryPlannerImpl() {
    super();
  }

  @Override
  public QueryPlan computeQueryPlan(BrokerRequest query, List<IndexSegment> indexSegmentList) {
    QueryPlanCreator queryPlanCreator = new QueryPlanCreator(query);
    queryPlanCreator.addJobVertexWithDependency(null, new JobVertex(indexSegmentList));
    return queryPlanCreator.buildQueryPlan();
  }
}

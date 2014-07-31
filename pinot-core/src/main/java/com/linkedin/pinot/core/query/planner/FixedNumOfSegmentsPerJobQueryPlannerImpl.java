package com.linkedin.pinot.core.query.planner;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.query.request.Query;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * An implementation of QueryPlanner, will mark all the segments with 0 degree in the graph. 
 *
 */
public class FixedNumOfSegmentsPerJobQueryPlannerImpl implements QueryPlanner {

  int _numSegmentsPerJob = 0;

  public FixedNumOfSegmentsPerJobQueryPlannerImpl() {
    super();
  }

  public FixedNumOfSegmentsPerJobQueryPlannerImpl(int numJobs) {
    super();
    setNumJobs(numJobs);
  }

  public void setNumJobs(int numSegmentsPerJob) {
    _numSegmentsPerJob = numSegmentsPerJob;
  }

  @Override
  public QueryPlan computeQueryPlan(Query query, List<IndexSegment> indexSegmentList) {
    QueryPlanCreator queryPlanCreator = new QueryPlanCreator(query);
    List<IndexSegment> vertexSegmentList = new ArrayList<IndexSegment>();
    int i = 0;
    for (IndexSegment indexSegment : indexSegmentList) {

      vertexSegmentList.add(indexSegment);

      if ((++i) % _numSegmentsPerJob == 0) {
        queryPlanCreator.addJobVertexWithDependency(null, new JobVertex(vertexSegmentList));
        vertexSegmentList = new ArrayList<IndexSegment>();
      }
    }
    if (vertexSegmentList.size() > 0) {
      queryPlanCreator.addJobVertexWithDependency(null, new JobVertex(vertexSegmentList));
    }
    return queryPlanCreator.buildQueryPlan();
  }
}

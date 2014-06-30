package com.linkedin.pinot.query.planner;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.server.partition.SegmentDataManager;


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
  public QueryPlan computeQueryPlan(Query query, List<SegmentDataManager> segmentDataManagerList) {
    QueryPlanCreator queryPlanCreator = new QueryPlanCreator(query);
    List<IndexSegment> indexSegmentList = new ArrayList<IndexSegment>();
    int i = 0;
    for (SegmentDataManager segmentDataManager : segmentDataManagerList) {

      indexSegmentList.add(segmentDataManager.getSegment());

      if ((++i) % _numSegmentsPerJob == 0) {
        queryPlanCreator.addJobVertexWithDependency(null, new JobVertex(indexSegmentList));
        indexSegmentList = new ArrayList<IndexSegment>();
      }
    }
    if (indexSegmentList.size() > 0) {
      queryPlanCreator.addJobVertexWithDependency(null, new JobVertex(indexSegmentList));
    }
    return queryPlanCreator.buildQueryPlan();
  }
}

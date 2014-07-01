package com.linkedin.pinot.query.planner;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.server.partition.SegmentDataManager;


/**
 * An implementation of QueryPlanner, will evenly distributed segments to a given number of jobs.
 *
 */
public class FixedNumJobsQueryPlannerImpl implements QueryPlanner {

  int _numJobs = 0;

  public FixedNumJobsQueryPlannerImpl() {
    super();
  }

  public FixedNumJobsQueryPlannerImpl(int numJobs) {
    super();
    setNumJobs(numJobs);
  }

  public void setNumJobs(int numJobs) {
    _numJobs = numJobs;
  }

  @Override
  public QueryPlan computeQueryPlan(Query query, List<SegmentDataManager> segmentDataManagerList) {
    QueryPlanCreator queryPlanCreator = new QueryPlanCreator(query);
    List<JobVertex> jobVertexList = new ArrayList<JobVertex>();
    for (int i = 0; i < _numJobs; ++i) {
      jobVertexList.add(new JobVertex(new ArrayList<IndexSegment>()));
    }
    int i = 0;
    for (SegmentDataManager segmentDataManager : segmentDataManagerList) {
      (jobVertexList.get(i++).getIndexSegmentList()).add(segmentDataManager.getSegment());
      if (i == _numJobs) {
        i = 0;
      }
    }
    for (JobVertex jobVertex : jobVertexList) {
      queryPlanCreator.addJobVertexWithDependency(null, jobVertex);
    }
    return queryPlanCreator.buildQueryPlan();
  }
}

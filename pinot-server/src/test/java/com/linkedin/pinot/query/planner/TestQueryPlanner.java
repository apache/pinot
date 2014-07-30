package com.linkedin.pinot.query.planner;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.linkedin.pinot.query.planner.JobVertex;
import com.linkedin.pinot.server.partition.SegmentDataManager;


public class TestQueryPlanner {

  public List<SegmentDataManager> _segmentDataManagers = null;
  public int _numOfSegmentDataManagers = 100;

  @Before
  public void init() {
    _segmentDataManagers = new ArrayList<SegmentDataManager>();
    for (int i = 0; i < _numOfSegmentDataManagers; ++i) {
      _segmentDataManagers.add(new SegmentDataManager(null));
    }
  }

  @Test
  public void testParallelQueryPlanner() {
    QueryPlanner queryPlanner = new ParallelQueryPlannerImpl();
    QueryPlan plan = queryPlanner.computeQueryPlan(null, _segmentDataManagers);
    List<JobVertex> roots = plan.getVirtualRoot().getSuccessors();
    assertEquals(_numOfSegmentDataManagers, roots.size());
  }

  @Test
  public void testSequentialQueryPlanner() {
    QueryPlanner queryPlanner = new SequentialQueryPlannerImpl();
    QueryPlan plan = queryPlanner.computeQueryPlan(null, _segmentDataManagers);
    List<JobVertex> roots = plan.getVirtualRoot().getSuccessors();
    assertEquals(1, roots.size());
    assertEquals(_numOfSegmentDataManagers, roots.get(0).getIndexSegmentList().size());
  }

  @Test
  public void testFixedJobsQueryPlanner() {
    for (int numJobs = 1; numJobs <= _numOfSegmentDataManagers; ++numJobs) {
      int totalSegments = 0;
      QueryPlanner queryPlanner = new FixedNumJobsQueryPlannerImpl(numJobs);
      QueryPlan plan = queryPlanner.computeQueryPlan(null, _segmentDataManagers);
      List<JobVertex> roots = plan.getVirtualRoot().getSuccessors();
      assertEquals(roots.size(), numJobs);

      for (int i = numJobs - 1; i >= 0; --i) {
        totalSegments += roots.get(0).getIndexSegmentList().size();
        roots.remove(0);
        assertEquals(roots.size(), i);
      }
      assertEquals(_numOfSegmentDataManagers, totalSegments);
    }
  }

  @Test
  public void testFixedSegmentsPerJobQueryPlanner() {
    for (int numSegment = 1; numSegment <= _numOfSegmentDataManagers; ++numSegment) {
      int totalSegments = 0;
      int numJobs = (int) Math.ceil((double) _numOfSegmentDataManagers / (double) numSegment);
      QueryPlanner queryPlanner = new FixedNumOfSegmentsPerJobQueryPlannerImpl(numSegment);
      QueryPlan plan = queryPlanner.computeQueryPlan(null, _segmentDataManagers);
      List<JobVertex> roots = plan.getVirtualRoot().getSuccessors();
      assertEquals(roots.size(), numJobs);
      for (int i = numJobs - 1; i >= 0; --i) {
        totalSegments += roots.get(0).getIndexSegmentList().size();
        roots.remove(0);
        assertEquals(roots.size(), i);
      }
      assertEquals(_numOfSegmentDataManagers, totalSegments);
    }
  }
}
